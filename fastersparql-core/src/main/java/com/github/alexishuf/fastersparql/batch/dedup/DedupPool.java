package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;

@SuppressWarnings("UnusedReturnValue")
public final class DedupPool<B extends Batch<B>> {
    private final BatchType<B> batchType;
    private final LIFOPool<WeakCrossSourceDedup<B>> weakCross;
    private final LIFOPool<WeakDedup<B>> weak;
    private final LIFOPool<StrongDedup<B>> reduced;
    private final LIFOPool<StrongDedup<B>> distinct;

    @SuppressWarnings("unchecked") public DedupPool(BatchType<B> batchType) {
        this.batchType = batchType;
        int threads = Runtime.getRuntime().availableProcessors();
        this.weakCross = (LIFOPool<WeakCrossSourceDedup<B>>)(Object)
                new LIFOPool<>(WeakCrossSourceDedup.class, 8*threads);
        this.weak = (LIFOPool<WeakDedup<B>>)(Object)
                new LIFOPool<>(WeakDedup.class, 8*threads);
        this.reduced = (LIFOPool<StrongDedup<B>>)(Object)
                new LIFOPool<>(StrongDedup.class, 2*threads);
        this.distinct = (LIFOPool<StrongDedup<B>>)(Object)
                new LIFOPool<>(StrongDedup.class, 2*threads);
    }

    /* --- --- --- get methods --- --- --- */

    public WeakCrossSourceDedup<B> getWeakCross(int capacity, int cols) {
        var d = weakCross.get();
        if (d == null)
            return new WeakCrossSourceDedup<>(batchType, capacity, cols);
        d.clear(cols);
        return d;
    }

    public WeakDedup<B> getWeak(int capacity, int cols) {
        var d = weak.get();
        if (d == null)
            return new WeakDedup<>(batchType, capacity, cols);
        d.clear(cols);
        return d;
    }

    public StrongDedup<B> getReduced(int weakenAt, int cols) {
        var d = reduced.get();
        if (d == null)
            return StrongDedup.strongUntil(batchType, weakenAt, cols);
        d.clear(cols);
        return d;
    }

    public StrongDedup<B> getDistinct(int weakenAt, int cols) {
        var d = distinct.get();
        if (d == null)
            return StrongDedup.strongUntil(batchType, weakenAt, cols);
        d.clear(cols);
        return d;
    }

    /* --- --- --- offer methods --- --- --- */

    public void recycleWeakCross(WeakCrossSourceDedup<B> d) {
        if (weakCross.offer(d) != null)
            d.recycleInternals();
    }
    public void recycleWeak(WeakDedup<B> d) {
        if (weak.offer(d) != null)
            d.recycleInternals();
    }
    public void recycleReduced(StrongDedup<B> d) {
        if (reduced.offer(d) != null)
            d.recycleInternals();
    }
    public void recycleDistinct(StrongDedup<B> d) {
        if (distinct.offer(d) != null)
            d.recycleInternals();
    }
}
