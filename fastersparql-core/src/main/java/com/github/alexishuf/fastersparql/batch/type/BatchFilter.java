package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
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
    protected final short outColumns;
    @SuppressWarnings("unused") private long plainReqLimit, plainDownReq;

    /* --- --- --- lifecycle --- --- --- */

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       Orphan<? extends RowFilter<B, ?>> rowFilter,
                       @Nullable Orphan<? extends BatchProcessor<B, ?>> before) {
        super(batchType, outVars, CREATED, PROC_FLAGS, before);
        this.rowFilter    = rowFilter.takeOwnership(this);
        this.bindableVars = this.rowFilter.bindableVars();
        this.outColumns   = (short)outVars.size();
        resetReqLimit();
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    private void resetReqLimit() {
        long limit = Long.MAX_VALUE;
        for (BatchProcessor<B, ?> p = this; p != null && limit == Long.MAX_VALUE; p = p.before) {
            if (p instanceof BatchFilter<?,?> bf)
                limit = bf.rowFilter.upstreamRequestLimit();
        }
        REQ_LIMIT.setRelease(this, limit);
        DOWN_REQ .setRelease(this, 0);
    }

    @Override protected void doRelease() {
        Owned.safeRecycle(rowFilter, this);
        super.doRelease();
    }

    /* --- --- --- Emitter methods --- --- --- */

    @Override public void rebind(BatchBinding binding) throws RebindException {
        super.rebind(binding);
        resetReqLimit();
        if (rowFilter != null)
            rowFilter.rebind(binding);
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
        if (before != null)
            sb.append('\n').append(before.label(type).replace("\n", "\n  "));
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
        if (rowFilter instanceof Dedup<?,?>)
            return true;
        for (BatchProcessor<B, ?> p = before; p != null; p = p.before) {
            if (p instanceof BatchFilter<?,?> bf && bf.rowFilter instanceof Dedup<?,?>)
                return true;
        }
        return false;
    }

    public abstract Orphan<B> filterInPlace(Orphan<B> in);

    protected B filterEmpty(@Nullable B in) {
        if (in == null) return null;
        short survivors = 0;
        for (var node = in; node != null; node = node.next) {
            for (int r = 0, rows = node.rows; r < rows; r++) {
                switch (rowFilter.drop(node, r)) {
                    case KEEP      -> ++survivors;
                    case TERMINATE -> rows = -1;
                }
            }
        }
        in.clear(outColumns);
        in.rows = survivors;
        return in;
    }
}
