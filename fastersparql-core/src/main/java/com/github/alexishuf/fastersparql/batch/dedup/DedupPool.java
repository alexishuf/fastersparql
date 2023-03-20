package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings("UnusedReturnValue")
public final class DedupPool<B extends Batch<B>> {
    private final BatchType<B> batchType;
    private final LIFOPool<Dedup<B>>[] pools;

    private static final int WEAK_CROSS = 0;
    private static final int WEAK = 1;
    private static final int REDUCED          = 2;
    private static final int DISTINCT         = 3;

    public DedupPool(BatchType<B> batchType) {
        this.batchType = batchType;
        //noinspection unchecked
        this.pools = new LIFOPool[] {
            LIFOPool.perProcessor(WeakCrossSourceDedup.class, 4), // WEAK_CROSS
            LIFOPool.perProcessor(WeakDedup.class, 8),            // WEAK
            LIFOPool.perProcessor(StrongDedup.class, 1),          // REDUCED
            LIFOPool.perProcessor(StrongDedup.class, 1),          // DISTINCT
        };
    }

    /* --- --- --- get methods --- --- --- */

    public WeakCrossSourceDedup<B> getWeakCross(int capacity, int cols) {
        WeakCrossSourceDedup<B> d = (WeakCrossSourceDedup<B>)pools[WEAK_CROSS].get();
        if (d == null || d.capacity() < capacity)
            return new WeakCrossSourceDedup<>(batchType, capacity, cols);
        d.clear(cols);
        return d;
    }

    public WeakDedup<B> getWeak(int capacity, int cols) {
        WeakDedup<B> d = (WeakDedup<B>) pools[WEAK].get();
        if (d == null || d.capacity() < capacity)
            return new WeakDedup<>(batchType, capacity, cols);
        d.clear(cols);
        return d;
    }

    private StrongDedup<B> getStrong(int type, int weakenAt, int cols) {
        StrongDedup<B> d = (StrongDedup<B>) pools[type].get();
        if (d == null || d.weakenAt() < weakenAt)
            return StrongDedup.strongUntil(batchType, weakenAt, cols);
        d.clear(cols);
        return d;
    }

    public StrongDedup<B> getReduced(int weakenAt, int cols) {
        return getStrong(REDUCED, weakenAt, cols);
    }

    public StrongDedup<B> getDistinct(int weakenAt, int cols) {
        return getStrong(DISTINCT, weakenAt, cols);
    }

    /* --- --- --- offer methods --- --- --- */

    public @Nullable WeakCrossSourceDedup<B> offerWeakCross(WeakCrossSourceDedup<B> d) {
        return (WeakCrossSourceDedup<B>)pools[WEAK_CROSS].offer(d);
    }
    public @Nullable WeakDedup<B> offerWeak(WeakDedup<B> d) {
        return (WeakDedup<B>)pools[WEAK].offer(d);
    }
    public @Nullable StrongDedup<B> offerReduced(StrongDedup<B> d) {
        return (StrongDedup<B>)pools[REDUCED].offer(d);
    }
    public @Nullable StrongDedup<B> offerDistinct(StrongDedup<B> d) {
        return (StrongDedup<B>)pools[DISTINCT].offer(d);
    }
}
