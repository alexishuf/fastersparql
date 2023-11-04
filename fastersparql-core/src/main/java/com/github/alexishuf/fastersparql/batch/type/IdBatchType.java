package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static java.lang.Thread.currentThread;

public abstract class IdBatchType<B extends IdBatch<B>> extends BatchType<B> {
    public static final short DEF_BATCH_TERMS = PREFERRED_BATCH_TERMS;

    private final LevelBatchPool<B> levelPool;

    public IdBatchType(Class<B> cls, BatchPool.Factory<B> factory,
                       LevelBatchPool.Factory<B> sizedFactory) {
        super(cls, factory);
        levelPool = new LevelBatchPool<>(sizedFactory, pool, DEF_BATCH_TERMS);
    }

    public final B createSpecial(int terms) {
        B b = levelPool.get(terms);
        BatchEvent.Unpooled.record(b.markUnpooled());
        return b;
    }
    public final @Nullable B recycleSpecial(B b) { return recycle(b); }

    @Override public B createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols);
    }

    @Override public B emptyForThread(int threadId, @Nullable B offer, int cols) {
        return emptyForThread0(threadId, offer).clear(cols);
    }

    @Override
    public @Nullable B recycleForThread(int threadId, @Nullable B b) {
        for (B next; b != null; b = next) {
            next = b.next;
            b.next = null;
            b.tail = b;
            BatchEvent.Pooled.record(b.markPooled());
            b = b.special ? levelPool.offer(b) : pool.offer(threadId, b);
            if (b != null) b.markGarbage();
        }
        return null;
    }

    @Override public B create(int cols) {
        return createForThread((int)currentThread().threadId(), cols);
    }
    @Override public B empty(@Nullable B offer, int cols) {
        return emptyForThread((int)currentThread().threadId(), offer, cols);
    }
    @Override public @Nullable B recycle(@Nullable B batch) {
        return recycleForThread((int)currentThread().threadId(), batch);
    }

    @Override public RowBucket<B> createBucket(int rows, int cols) {
        return new IdBatchBucket<>(this, rows, cols);
    }

    @Override public IdBatch.@Nullable Merger<B> projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new IdBatch.Merger<>(this, out, sources);
    }

    @Override public IdBatch.@NonNull Merger<B> merger(Vars out, Vars left, Vars right) {
        return new IdBatch.Merger<>(this, out, mergerSources(out, left, right));
    }

    @Override public IdBatch.Filter<B> filter(Vars out, Vars in, RowFilter<B> filter,
                                                       BatchFilter<B> before) {
        return new IdBatch.Filter<>(this, out, projector(out, in), filter, before);
    }

    @Override public IdBatch.Filter<B> filter(Vars vars, RowFilter<B> filter,
                                                       BatchFilter<B> before) {
        return new IdBatch.Filter<>(this, vars, null, filter, before);
    }
}
