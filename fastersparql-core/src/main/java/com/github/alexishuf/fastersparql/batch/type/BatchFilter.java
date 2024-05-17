package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.MINIMAL;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;
import static java.lang.Math.max;
import static java.lang.Math.min;

public abstract class BatchFilter<B extends Batch<B>, P extends BatchFilter<B, P>>
        extends BatchProcessor<B, P> {
    private static final VarHandle REQ_LIMIT, DOWN_REQ;

    static {
        try {
            REQ_LIMIT = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainReqLimit", long.class);
            DOWN_REQ  = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainDownReq",  long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public final RowFilter<B, ?> rowFilter;
    public final @Nullable BatchFilter<B, ?> before;
    protected final short outColumns;
    @SuppressWarnings("unused") private long plainReqLimit, plainDownReq;

    /* --- --- --- lifecycle --- --- --- */

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       Orphan<? extends RowFilter<B, ?>> rowFilter,
                       @Nullable Orphan<? extends BatchFilter<B, ?>> before) {
        super(batchType, outVars, CREATED, PROC_FLAGS);
        this.rowFilter    = rowFilter.takeOwnership(this);
        this.bindableVars = this.rowFilter.bindableVars();
        this.before       = before == null ? null : before.takeOwnership(this);
        this.outColumns   = (short)outVars.size();
        resetReqLimit();
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    private void resetReqLimit() {
        long limit = Long.MAX_VALUE;
        for (BatchFilter<B, ?> bf = this; bf != null && limit == Long.MAX_VALUE; bf = bf.before)
            limit = bf.rowFilter.upstreamRequestLimit();
        REQ_LIMIT.setRelease(this, limit);
        DOWN_REQ .setRelease(this, 0);
    }

    @Override protected void doRelease() {
        try {
            rowFilter.recycle(this);
            if (before != null) before.recycle(this);
        } finally {
            super.doRelease();
        }
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

    @Override protected void afterOnBatch(@Nullable Orphan<B> orphan, long receivedRows) {
        boolean request = false;
        if (orphan != null) {
            long survivors = Batch.peekTotalRows(orphan);
            DOWN_REQ.getAndAddRelease(this, -survivors);
            if ((long)REQ_LIMIT.getAndAddRelease(this, -survivors)-survivors <= 0)
                cancelUpstream();
            else if (survivors < receivedRows)
                request = true;
        }
        super.afterOnBatch(orphan, receivedRows);
        if (request) {
            var up = upstream;
            if (up != null)
                up.request((long)DOWN_REQ.getOpaque(this));
        }
    }

    /* --- --- --- BatchProcessor methods --- --- --- */

    public final boolean isDedup() {
        return rowFilter instanceof Dedup<?, ?> || (before != null && before.isDedup());
    }

    public abstract Orphan<B> filterInPlace(Orphan<B> in);

    protected final B filterInPlaceSkipEmpty(B b, B prev) {
        prev.next = b.dropHead(b == prev ? this : prev);
        return prev;
    }

    protected final B filterInPlaceEpilogue(B in, B last) {
        if (in != null) {
            in  .tail = last;
            last.tail = last;
            if (in.rows == 0) {
                if (in.next == null) in.cols = outColumns;
                else                 in = in.dropHead(this);
            }
            assert in == null || in.validate();
        }
        return in;
    }

    protected B filterEmpty(@Nullable B in) {
        if (in == null) return null;
        short survivors = 0;
        for (int r = 0, rows = in.rows; r < rows; r++) {
            switch (rowFilter.drop(in, r)) {
                case KEEP      -> ++survivors;
                case TERMINATE -> rows = -1;
            }
        }
        in.rows = survivors;
        in.cols = outColumns;
        return in;
    }
}
