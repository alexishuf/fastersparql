package com.github.alexishuf.fastersparql.batch.type;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;

import static java.lang.invoke.MethodHandles.lookup;

public abstract class BatchProcessor<B extends Batch<B>> {
    private static final VarHandle RECYCLED;

    static {
        try {
            RECYCLED = lookup().findVarHandle(BatchProcessor.class, "recycled", Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected @Nullable B recycled;
    protected BatchType<B> batchType;

    public BatchProcessor(BatchType<B> batchType) {
        this.batchType = batchType;
    }

    /**
     * Notifies that this processor will not be used anymore and that it may release
     * resources it holds (e.g., pooled objects).
     */
    public void release() {
        batchType.recycle(stealRecycled());
    }

    /**
     * Tries to process {@code b} in-place avoiding new allocations. If this cannot be done,
     * {@link BatchProcessor#process(Batch)} will internally be called and a reference to
     * another batch with the result will be returned.
     *
     * <p>In both cases, the caller gives up ownership of {@code b}. If {@code b} cannot be
     * processed in-place it will be recycled.</p>
     *
     * @param b the batch to process
     * @return a batch with the result
     */
    public abstract B processInPlace(B b);

    /**
     * Returns a batch with the result of processing {@code b}. Ownership of {@code b} remains
     * with the caller and this method does not mutate {@code b}.
     *
     * @param b the batch to process.
     * @return a batch with processing results.
     */
    public abstract B process(B b);

    /**
     * Offers a batch for recycling and reuse in {@link BatchProcessor#process(Batch)}.
     *
     * @param batch a batch to recycle
     * @return {@code null} if the batch is now owned by the {@link BatchProcessor},
     *         {@code batch} if ownership remains with caller
     */
    public final @Nullable B recycle(@Nullable B batch) {
        return RECYCLED.compareAndExchange(this, null, batch) == null ? null : batch;
    }

    /**
     * Takes ownership of a previously {@link BatchProcessor#recycle(Batch)}ed batch.
     *
     * <p>The use case is to move the batch somewhere else when the {@link BatchProcessor}
     * is about to become unreachable </p>
     *
     * @return a previously {@link BatchProcessor#recycle(Batch)}ed batch or {@code null}
     */
    public final @Nullable B stealRecycled() {
        //noinspection unchecked
        return (B)RECYCLED.getAndSet(this, null);
    }

    protected final B getBatch(int rowsCapacity, int cols, int bytesCapacity) {
        //noinspection unchecked
        B b = (B) RECYCLED.getAndSet(this, null);
        if (b == null)
            return batchType.create(rowsCapacity, cols, bytesCapacity);
        b.clear(cols);
        return b;
    }
}
