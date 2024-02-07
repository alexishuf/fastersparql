package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.MINIMAL;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;
import static java.lang.Math.max;
import static java.lang.Math.min;

public abstract class BatchFilter<B extends Batch<B>> extends BatchProcessor<B> {
    private static final VarHandle REQ_LIMIT, DOWN_REQ;

    static {
        try {
            REQ_LIMIT = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainReqLimit", long.class);
            DOWN_REQ  = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainDownReq",  long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public final RowFilter<B> rowFilter;
    public final @Nullable BatchFilter<B> before;
    protected final short outColumns;
    @SuppressWarnings("unused") private long plainReqLimit, plainDownReq;

    /* --- --- --- lifecycle --- --- --- */

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
        super(batchType, outVars, CREATED, PROC_FLAGS);
        this.rowFilter    = rowFilter;
        this.bindableVars = rowFilter.bindableVars();
        this.before       = before;
        this.outColumns   = (short)outVars.size();
        resetReqLimit();
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    private void resetReqLimit() {
        long limit = Long.MAX_VALUE;
        for (var bf = this; bf != null && limit == Long.MAX_VALUE; bf = bf.before)
            limit = bf.rowFilter.upstreamRequestLimit();
        REQ_LIMIT.setRelease(this, limit);
        DOWN_REQ .setRelease(this, 0);
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
        var rf = rowFilter;
        if (rf     != null)     rf.rebind(binding);
        if (before != null) before.rebind(binding);
    }

    @Override public void request(long downstreamRequest) throws NoReceiverException {
        if (downstreamRequest <= 0)
            return;
        long rows = max(1, min(downstreamRequest, (long)REQ_LIMIT.getAcquire(this)));
        if (Async.maxRelease(DOWN_REQ, this, rows))
            super.request(rows);
    }

    @Override public String toString() {
        return label(MINIMAL)+'('+(upstream==null ? "null" : upstream.label(MINIMAL))+')';
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = new StringBuilder();
        if (type == MINIMAL)
            return StreamNodeDOT.minimalLabel(sb, this).toString();
        sb.append(rowFilter).append('@');
        sb.append(Integer.toHexString(System.identityHashCode(this)));
        if (type.showState()) {
            sb.append("\nstate=").append(flags.render(state()))
                    .append(", upstreamCancelled=").append(upstreamCancelled());
            appendRequested(sb.append(", requestLimit="), (long)REQ_LIMIT.getAcquire(this));
            appendRequested(sb.append(", requested="), (long) DOWN_REQ.getAcquire(this));
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

    @Override protected @Nullable B afterOnBatch(@Nullable B batch, long receivedRows) {
        boolean request = false;
        if (batch != null) {
            long survivors = batch.totalRows();
            DOWN_REQ.getAndAddRelease(this, -survivors);
            if ((long)REQ_LIMIT.getAndAddRelease(this, -survivors)-survivors <= 0)
                cancelUpstream();
            else if (survivors < receivedRows)
                request = true;
        }
        B offer = super.afterOnBatch(batch, receivedRows);
        if (request)
            upstream.request((long)DOWN_REQ.getOpaque(this));
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
