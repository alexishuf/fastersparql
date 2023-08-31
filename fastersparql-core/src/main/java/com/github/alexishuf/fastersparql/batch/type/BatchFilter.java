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

    private static final int DEDUP_TYPE_MASK = 0x00030000;
    private static final int DEDUP_COLS_MASK = 0xfffc0000;
    private static final Flags FILTER_FLAGS = Flags.DEFAULT.toBuilder()
            .counter(DEDUP_TYPE_MASK, "dedup")
            .counter(DEDUP_COLS_MASK, "dedupCols")
            .build();
    private static final int DEDUP_TYPE_BIT      = numberOfTrailingZeros(DEDUP_TYPE_MASK);
    private static final int DEDUP_COLS_BIT      = numberOfTrailingZeros(DEDUP_COLS_MASK);
    private static final int DEDUP_WEAK_TYPE     = 0x0;
    private static final int DEDUP_CROSS_TYPE    = 0x1;
    private static final int DEDUP_REDUCED_TYPE  = 0x2;
    private static final int DEDUP_DISTINCT_TYPE = 0x3;
    private static final int MAX_DEDUP_COLS      = DEDUP_COLS_MASK >>> DEDUP_COLS_BIT;

    public final @Nullable BatchMerger<B> projector;
    public RowFilter<B> rowFilter;
    public final @Nullable BatchFilter<B> before;
    @SuppressWarnings("unused") protected long requestLimit, plainUpRequested;

    /* --- --- --- lifecycle --- --- --- */

    private static int initState(RowFilter<?> filter) {
        int state = CREATED;
        if (filter instanceof Dedup<?> dedup) {
            int cols = dedup.cols();
            if (cols > MAX_DEDUP_COLS)
                throw new IllegalArgumentException("Too many dedup cols");
            state |= cols << DEDUP_COLS_BIT;
            if (filter instanceof StrongDedup<?> d) {
                if (d.weakenAt() >= FSProperties.distinctCapacity())
                    state |= DEDUP_DISTINCT_TYPE << DEDUP_TYPE_BIT;
                else
                    state |= DEDUP_REDUCED_TYPE << DEDUP_TYPE_BIT;
            } else if (filter instanceof WeakCrossSourceDedup<?>) {
                state |= DEDUP_CROSS_TYPE << DEDUP_TYPE_BIT;
            } else {
                assert filter instanceof WeakDedup<?> : "unexpected Dedup type";
                state |= DEDUP_WEAK_TYPE << DEDUP_TYPE_BIT;
            }
        }
        return state;
    }

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       @Nullable BatchMerger<B> projector,
                       RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
        super(batchType, outVars, initState(rowFilter), FILTER_FLAGS);
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

    /* --- --- --- Emitter methods --- --- --- */

    @Override public void rebindAcquire() {
        super.rebindAcquire();
        var rf = rowFilter;
        if (rf     != null) rf    .rebindAcquire();
        if (before != null) before.rebindAcquire();
    }

    @Override public void rebindRelease() {
        super.rebindRelease();
        var rf = rowFilter;
        if (rf     != null) rf    .rebindRelease();
        if (before != null) before.rebindRelease();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        super.rebind(binding);
        var rf = rowFilter;
        if (rf     != null)     rf.rebind(binding);
        if (before != null) before.rebind(binding);
    }

    @Override public void request(long rows) throws NoReceiverException {
        if (upstream == null) throw new NoUpstreamException(this);
        rows = Math.max(1, Math.min(rows, (long)REQUEST_LIMIT.getOpaque(this)));
        Async.safeAddAndGetRelease(UP_REQUESTED, this, rows);
        upstream.request(rows);
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
