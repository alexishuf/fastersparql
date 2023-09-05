package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.emit.exceptions.NoUpstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

import static java.lang.Integer.numberOfTrailingZeros;

public abstract class BatchFilter<B extends Batch<B>> extends BatchProcessor<B> {
    private static final VarHandle UP_REQUESTED, REQUEST_LIMIT;
    static {
        try {
            UP_REQUESTED   = MethodHandles.lookup().findVarHandle(BatchFilter.class, "plainUpRequested",  long.class);
            REQUEST_LIMIT  = MethodHandles.lookup().findVarHandle(BatchFilter.class, "requestLimit", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public final @Nullable BatchMerger<B> projector;
    public final RowFilter<B> rowFilter;
    public final @Nullable BatchFilter<B> before;
    @SuppressWarnings("unused") protected long requestLimit, plainUpRequested;

    /* --- --- --- lifecycle --- --- --- */

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       @Nullable BatchMerger<B> projector,
                       RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
        super(batchType, outVars, CREATED, Flags.DEFAULT);
        this.projector = projector;
        this.rowFilter = rowFilter;
        this.before = before;
        requestLimit = Long.MAX_VALUE;
        for (var bf = this; bf != null && requestLimit == Long.MAX_VALUE; bf = bf.before)
            requestLimit = bf.rowFilter.upstreamRequestLimit();
        if (requestLimit < Long.MAX_VALUE)
            ++requestLimit;
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
        var rf = rowFilter;
        if (rf     != null)     rf.rebind(binding);
        if (before != null) before.rebind(binding);
    }

    @Override public void request(long rows) throws NoReceiverException {
        rows = Math.max(1, Math.min(rows, (long)REQUEST_LIMIT.getOpaque(this)));
        Async.safeAddAndGetRelease(UP_REQUESTED, this, rows);
        super.request(rows);
    }

    @Override public String toString() {
        return nodeLabel() + "<-" + upstream;
    }

    @Override public String nodeLabel() {
        String rf = rowFilter.toString();
        return before == null ? rf : rf+"<-"+before.nodeLabel();
    }

    /* --- --- --- Receiver methods --- --- --- */

    @Override public @Nullable B onBatch(B batch) {
        if (batch == null) return null;
        REQUEST_LIMIT.getAndAddRelease(this, (long)-batch.rows);
        return super.onBatch(batch);
    }

    /* --- --- --- BatchProcessor methods --- --- --- */

    @Override public final B processInPlace(B b) { return filterInPlace(b, projector); }

    public final boolean isDedup() {
        return rowFilter instanceof Dedup<B> || (before != null && before.isDedup());
    }

    public abstract B filterInPlace(B in, @Nullable BatchMerger<B> projector);

    public final B filterInPlace(B in) { return filterInPlace(in, projector); }

    public B filter(@Nullable B dest, B in) {
        if (before != null)
            in = before.filter(null, in);
        if (in == null)
            return null;
        int rows = in.rows;
        BatchMerger<B> projector = this.projector;
        if (dest == null) {
            int cols = projector == null ? in.cols : projector.sources.length;
            dest = getBatch(rows, cols, in.localBytesUsed());
        }
        if (rowFilter.targetsProjection() && projector != null) {
            dest = projector.project(dest, in);
            return filterInPlace(dest, null);
        }
        if (projector == null) {
            for (int r = 0; r < rows; r++) {
                switch (rowFilter.drop(in, r)) {
                    case KEEP      -> dest.putRow(in, r);
                    case DROP      -> {}
                    case TERMINATE -> rows = -1;
                }
            }
        } else {
            int[] columns = Objects.requireNonNull(projector.columns);
            for (int r = 0; r < rows; r++) {
                switch (rowFilter.drop(in, r)) {
                    case KEEP -> {
                        dest.beginPut();
                        for (int c = 0, s; c < columns.length; c++) {
                            if ((s = columns[c]) >= 0)
                                dest.putTerm(c, in, r, s);
                        }
                        dest.commitPut();
                    }
                    case DROP      -> {}
                    case TERMINATE -> rows = -1;
                }
            }
        }
        if (rows == -1) {
            cancelUpstream();
            if (dest.rows == 0) dest = batchType.recycle(dest);
        }
        return dest;
    }
}
