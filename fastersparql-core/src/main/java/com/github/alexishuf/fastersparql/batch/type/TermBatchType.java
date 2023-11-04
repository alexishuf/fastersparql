package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.TermBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.TermBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static java.lang.Thread.currentThread;

public final class TermBatchType extends BatchType<TermBatch> {
    public static final short DEF_BATCH_TERMS = PREFERRED_BATCH_TERMS*2;
    public static final TermBatchType TERM = new TermBatchType();

    private final LevelBatchPool<TermBatch> levelPool;

    @SuppressWarnings("SameReturnValue") public static TermBatchType get() { return TERM; }

    private static final class TermBatchFactory implements BatchPool.Factory<TermBatch> {
        @Override public TermBatch create() {
            var b = new TermBatch(new Term[DEF_BATCH_TERMS], 0, 1, false);
            b.markPooled();
            return b;
        }
    }

    private static final class SizedTermBatchFactory implements LevelBatchPool.Factory<TermBatch> {
        @Override public TermBatch create(int terms) {
            TermBatch b = new TermBatch(new Term[terms], 0, 1, true);
            b.markPooled();
            return b;
        }
    }

    private TermBatchType() {
        super(TermBatch.class, new TermBatchFactory());
        levelPool = new LevelBatchPool<>(new SizedTermBatchFactory(), pool, DEF_BATCH_TERMS);
    }

    public TermBatch createSpecial(int terms) {
        TermBatch b = levelPool.get(terms);
        BatchEvent.Unpooled.record(b.markUnpooled());
        return b;
    }

    public @Nullable TermBatch recycleSpecial(TermBatch b) { return recycle(b); }

    @Override public TermBatch createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols);
    }

    @Override public TermBatch emptyForThread(int threadId, @Nullable TermBatch offer, int cols) {
        return emptyForThread0(threadId, offer).clear(cols);
    }

    @Override public @Nullable TermBatch recycleForThread(int threadId, @Nullable TermBatch b) {
        for (TermBatch next; b != null; b = next) {
            next = b.next;
            Arrays.fill(b.arr, 0, b.rows*b.cols, null);
            b.rows = 0;
            b.next = null;
            b.tail = b;
            BatchEvent.Pooled.record(b.markPooled());
            b = b.special ? levelPool.offer(b) : pool.offer(threadId, b);
            if (b != null) b.markGarbage();
        }
        return null;
    }

    @Override public TermBatch create(int cols) {
        return createForThread((int)currentThread().threadId(), cols);
    }
    @Override public TermBatch empty(@Nullable TermBatch offer, int cols) {
        return emptyForThread((int)currentThread().threadId(), offer, cols);
    }
    @Override public @Nullable TermBatch recycle(@Nullable TermBatch b) {
        return recycleForThread((int)currentThread().threadId(), b);
    }

    @Override public short preferredTermsPerBatch() {
        return DEF_BATCH_TERMS;
    }

    @Override public RowBucket<TermBatch> createBucket(int rowsCapacity, int cols) {
        return new TermBatchBucket(rowsCapacity, cols);
    }

    @Override
    public @Nullable Merger projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger(this, out, sources);
    }

    @Override
    public @NonNull Merger merger(Vars out, Vars left, Vars right) {
        return new Merger(this, out, mergerSources(out, left, right));
    }

    @Override public Filter filter(Vars out, Vars in, RowFilter<TermBatch> filter,
                                   BatchFilter<TermBatch> before) {
        return new Filter(this, out, projector(out, in), filter, before);
    }

    @Override public Filter filter(Vars vars, RowFilter<TermBatch> filter,
                                   BatchFilter<TermBatch> before) {
        return new Filter(this, vars, null, filter, before);
    }
}
