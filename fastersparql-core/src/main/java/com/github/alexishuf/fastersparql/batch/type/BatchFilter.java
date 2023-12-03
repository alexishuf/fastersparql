package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.safeAddAndGetRelease;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Math.max;
import static java.lang.Math.min;

public abstract class BatchFilter<B extends Batch<B>> extends BatchProcessor<B> {
    private static final VarHandle REQ_LIMIT, REQ, PENDING;
    static {
        try {
            REQ_LIMIT = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainReqLimit", long.class);
            REQ       = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainReq",      long.class);
            PENDING   = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainPending",  long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public final RowFilter<B> rowFilter;
    public final @Nullable BatchFilter<B> before;
    protected final short outColumns;
    private final short chunk;
    @SuppressWarnings("unused") private long plainReqLimit, plainReq, plainPending;

    /* --- --- --- lifecycle --- --- --- */

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
        super(batchType, outVars, CREATED, PROC_FLAGS);
        this.rowFilter    = rowFilter;
        this.bindableVars = rowFilter.bindableVars();
        this.before       = before;
        this.outColumns   = (short)outVars.size();
        this.chunk        = (short)(batchType.preferredTermsPerBatch()/max(1, outColumns));
        resetReqLimit();
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    private void resetReqLimit() {
        long limit = Long.MAX_VALUE;
        for (var bf = this; bf != null && limit == Long.MAX_VALUE; bf = bf.before)
            limit = bf.rowFilter.upstreamRequestLimit();
        REQ_LIMIT.setRelease(this, limit);
        REQ      .setRelease(this, 0);
    }

    @Override protected void doRelease() {
        try {
            rowFilter.release();
            if (before != null) before.release();
        } finally {
            super.doRelease();
        }
    }

    @Override public void rebindAcquire() {
        super.rebindAcquire();
        if (rowFilter != null) rowFilter.rebindAcquire();
        if (before    != null) before.rebindAcquire();
    }

    @Override public void rebindRelease() {
        super.rebindRelease();
        if (rowFilter != null) rowFilter.rebindRelease();
        if (before    != null) before.rebindRelease();
    }

    /* --- --- --- Emitter methods --- --- --- */

    @Override public void rebind(BatchBinding binding) throws RebindException {
        super.rebind(binding);
        resetReqLimit();
        PENDING.setRelease(this, 0L);
        var rf = rowFilter;
        if (rf     != null)     rf.rebind(binding);
        if (before != null) before.rebind(binding);
    }

    @Override public void request(long downstreamRequest) throws NoReceiverException {
        if (downstreamRequest <= 0)
            return;
        long rows = max(1, min(downstreamRequest, (long)REQ_LIMIT.getAcquire(this)));
        if (ENABLED)
            journal("request rows=", rows, "reqLimit=", plainReqLimit, "on", this);
        safeAddAndGetRelease(REQ,     this, plainReq,     rows); // awaited  by   downstream
        safeAddAndGetRelease(PENDING, this, plainPending, rows); // awaiting from   upstream
        super.request(rows);
    }

    @Override public String toString() {
        return label(StreamNodeDOT.Label.MINIMAL) + "<-" + upstream;
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = new StringBuilder();
        if (type == StreamNodeDOT.Label.MINIMAL)
            return StreamNodeDOT.minimalLabel(sb, this).toString();
        sb.append(rowFilter).append('@');
        sb.append(Integer.toHexString(System.identityHashCode(this)));
        if (type.showState()) {
            sb.append("\nstate=").append(flags.render(state()))
                    .append(", upstreamCancelled=").append(upstreamCancelled());
            appendRequested(sb.append(", requestLimit="), (long)REQ_LIMIT.getAcquire(this));
            appendRequested(sb.append(", requested="), (long)REQ.getAcquire(this));
            appendRequested(sb.append(", pending="), (long)PENDING.getOpaque(this));
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        if (before != null) {
            sb.append('\n');
            sb.append(before.label(type).replace("\n", "\n  "));
        }
        return sb.toString();
    }

    /* --- --- --- Receiver methods --- --- --- */

    @Override protected @Nullable B onBatchEpilogue(@Nullable B batch, long receivedRows) {
        if (batch != null) {
            long delta = -batch.totalRows();
            REQ.getAndAddRelease(this, delta);
            if ((long)REQ_LIMIT.getAndAddRelease(this, delta)+delta <= 0)
                cancelUpstream();
        }
        B offer = super.onBatchEpilogue(batch, receivedRows);

        // request from upstream if pending requests are nearing exhaustion and are
        // below what was requested from this filter by downstream
        long reqSize, pending = (long)PENDING.getAndAddAcquire(this, -receivedRows)-receivedRows;
        if (pending <= chunk>>1 && (reqSize=plainReq-pending) > 0)
            upstream.request(max(min(plainReqLimit, chunk), reqSize)); // request at least chunk
        return offer;
    }

    /* --- --- --- BatchProcessor methods --- --- --- */

    public final boolean isDedup() {
        return rowFilter instanceof Dedup<B> || (before != null && before.isDedup());
    }

    public abstract B filterInPlace(B in);

    protected final B filterInPlaceSkipEmpty(B b, B prev) {
        prev.next = b.dropHead();
        return prev;
    }

    protected final B filterInPlaceEpilogue(B in, B last) {
        if (in != null) {
            in  .tail = last;
            last.tail = last;
            if (in.rows == 0) {
                if (in.next == null) in.cols = outColumns;
                else                 in = in.dropHead();
            }
            assert in == null || in.validate();
        }
        return in;
    }

    protected B filterEmpty(@Nullable B dst, B originalIn, B in) {
        if (in == null) return null;
        short survivors = 0;
        for (int r = 0, rows = in.rows; r < rows; r++) {
            switch (rowFilter.drop(in, r)) {
                case KEEP      -> ++survivors;
                case TERMINATE -> rows = -1;
            }
        }
        if (dst == in) {
            dst.rows = survivors;
        } else {
            if (dst == null)
                dst = batchType.create(outColumns);
            else if (dst.rows > 0 && dst.cols != outColumns)
                throw new IllegalArgumentException("dst not empty and dst.cols != outColumns");
            dst.rows += survivors;
        }
        dst.cols = outColumns;
        if (in != originalIn && in != dst)
            in.recycle();
        return dst;
    }
}
