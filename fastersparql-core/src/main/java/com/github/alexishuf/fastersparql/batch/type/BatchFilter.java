package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;

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
        super(batchType, outVars, CREATED, PROC_FLAGS);
        this.projector = projector;
        this.rowFilter = rowFilter;
        this.before = before;
        requestLimit = Long.MAX_VALUE;
        for (var bf = this; bf != null && requestLimit == Long.MAX_VALUE; bf = bf.before)
            requestLimit = bf.rowFilter.upstreamRequestLimit();
        if (requestLimit < Long.MAX_VALUE)
            ++requestLimit;
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
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
        return label(StreamNodeDOT.Label.WITH_STATE_AND_STATS) + "<-" + upstream;
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
            appendRequested(sb.append(", requestLimit="), (long)REQUEST_LIMIT.getOpaque(this));
            appendRequested(sb.append(", upRequested="), (long)UP_REQUESTED.getOpaque(this));
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

    @Override public @Nullable B onBatch(B batch) {
        if (batch == null) return null;
        REQUEST_LIMIT.getAndAddRelease(this, (long)-batch.rows);
        return super.onBatch(batch);
    }

    @Override public void onRow(B batch, int row) {
        if (batch == null)
            return;
        REQUEST_LIMIT.getAndAddRelease(this, -1L);
        boolean trivial = !rowFilter.targetsProjection();
        if (trivial && before != null) {
            if (before.before != null || before.rowFilter.targetsProjection()) {
                trivial = false;
            } else {
                switch (before.rowFilter.drop(batch, row)) {
                    case DROP      -> { return; }
                    case TERMINATE -> { cancelUpstream(); return; }
                }
            }
        }
        if (trivial) {
            switch (rowFilter.drop(batch, row)) {
                case KEEP -> {
                    if (EmitterStats.ENABLED && stats != null)
                        stats.onRowDelivered();
                    if (ResultJournal.ENABLED)
                        ResultJournal.logRow(this, batch, row);
                    downstream.onRow(batch, row);
                }
                case TERMINATE -> cancelUpstream();
            }
        } else {
            super.onRow(batch, row);
        }
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
