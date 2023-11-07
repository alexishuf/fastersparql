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

    public final RowFilter<B> rowFilter;
    public final @Nullable BatchFilter<B> before;
    protected final short outColumns;
    @SuppressWarnings("unused") protected long requestLimit, plainUpRequested;

    /* --- --- --- lifecycle --- --- --- */

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
        super(batchType, outVars, CREATED, PROC_FLAGS);
        this.rowFilter = rowFilter;
        this.bindableVars = rowFilter.bindableVars();
        this.before = before;
        this.outColumns = (short)outVars.size();
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

    @Override protected void onBatchPrologue(B batch) {
        super.onBatchPrologue(batch);
        if (batch != null)
            REQUEST_LIMIT.getAndAddRelease(this, (long)-batch.totalRows());
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
