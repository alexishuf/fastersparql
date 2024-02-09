package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.operators.ConverterBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public abstract class BatchType<B extends Batch<B>> implements BatchConverter<B> {
    protected static final int POOL_THREADS = Runtime.getRuntime().availableProcessors();
    protected static final int POOL_SHARED = POOL_THREADS*2048;
    /**
     * Preferred number of terms in the default size of batches obtained via
     * {@link #create(int)} and other related methods. {@link BatchType} implementations may
     * use other values, but using this values prevents situations where a single batch yields
     * more than one batch when converted to another type.
     */
    protected static final short PREFERRED_BATCH_TERMS = 256;

    @SuppressWarnings("unused") private static int plainNextId;
    public static final int MAX_BATCH_TYPE_ID = 63;
    private static final VarHandle NEXT_ID;
    static {
        try {
            NEXT_ID = MethodHandles.lookup().findStaticVarHandle(BatchType.class, "plainNextId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final BatchPool<B> pool;
    /** A 0-based auto-increment id unique to this {@link BatchType} instance. */
    public final int id;
    private final String name;

    public BatchType(Class<B> cls, BatchPool.Factory<B> factory) {
        this.id = (int)NEXT_ID.getAndAddRelease(1);
        if (this.id > MAX_BATCH_TYPE_ID)
            throw new IllegalStateException("Too many BatchType instances");
        this.pool = new BatchPool<>(cls, factory, POOL_THREADS, POOL_SHARED);
        this.name = getClass().getSimpleName().replaceAll("Type$", "");
    }

    /** Get the {@link Class} object of {@code B}. */
    @SuppressWarnings("unused") public final Class<B> batchClass() { return pool.batchClass(); }


    /**
     * Create or get a pooled batch that is empty and set for {@code cols} columns.
     *
     * @param cols desired {@link Batch#cols} of the returned batch
     * @return an empty {@link Batch}
     */
    public abstract B create(int cols);

    /**
     * Equivalent to {@link #create(int)} if {@code offer == null},
     * else return {@code offer} after {@link Batch#clear(int)}.
     *
     * @param offer possibly null batch to be used before trying the global pool.
     * @param cols number of columns in the batch returned by this method.
     * @return {@code offer}, cleared with {@code cols} columns or a new batch with space
     *         to fit {@code rows} and {@code localBytes} bytes of local segments.
     */
    public abstract B empty(@Nullable B offer,  int cols);

    /**
     * Equivalent to {@link #create(int)}, but will internally use {@code threadId} as a
     * surrogate for {@code Thread.currentThread().threadId()}.
     *
     * @param threadId a surrogate for {@link Thread#threadId()} of the calling thread. Need not
     *                 be accurate nor originating from a {@link Thread#threadId()} call.
     *                 If this value is not constant for the caller thread or if it is not as
     *                 unique as {@link Thread#threadId()} performance may be slightly degraded
     *                 due to increased CPU data cache misses and invalidation.
     * @param cols see {@link #create(int)}
     * @return see {@link #create(int)}
     */
    public abstract B createForThread(int threadId, int cols);

    /**
     * Equivalent to {@link #empty(Batch, int)}, but will internally use {@code threadId} as a
     * surrogate for {@code Thread.currentThread().threadId()}.
     *
     * @param threadId see {@link #createForThread(int, int)}
     * @param offer see {@link #empty(Batch, int)}
     * @param cols see {@link #empty(Batch, int)}
     * @return see {@link #empty(Batch, int)}
     */
    public abstract B emptyForThread(int threadId, @Nullable B offer, int cols);

    protected final B createForThread0(int threadId) {
        B b = pool.get(threadId);
        BatchEvent.Unpooled.record(b.markUnpooled());
        return b;
    }

    protected final B emptyForThread0(int threadId, @Nullable B offer) {
        if (offer == null) {
            offer = pool.get(threadId);
            offer.markUnpooled();
            BatchEvent.Unpooled.record(offer);
        }
        return offer;
    }

    /**
     * Get a {@link BIt} that produces the same batches as other, but with {@code this} type.
     */
    public <O extends Batch<O>> BIt<B> convert(BIt<O> other) {
        if (equals(other.batchType())) //noinspection unchecked
            return (BIt<B>) other;
        return new ConverterBIt<>(other, this);
    }

    public <I extends Batch<I>> Emitter<B> convert(Emitter<I> emitter) {
        if (equals(emitter.batchType())) //noinspection unchecked
            return (Emitter<B>) emitter;
        return new ConverterStage<>(this, emitter);
    }

    /**
     * Convert/copy a single batch.
     *
     * @param src original batch
     * @return a batch of this type ({@code B}) with the same rows as {@code src}
     */
    public <O extends Batch<O>> B convert(O src) {
        B b = create(src.cols);
        b.putConverting(src);
        return b;
    }

    /**
     * Offer a batch for recycling so that it may be returned in future {@link #create(int)}
     * (and similar) calls.
     *
     * <p>If the batch has a linked successor ({@link Batch#next()}), <strong>it and all
     * transitive successors WILL ALSO BE RECYCLED</strong></p>
     *
     * @param batch the {@link Batch} to be recycled
     * @return {@code null}, for conveniently setting recycled references to null:
     *         {@code b = type.recycle(b)}
     */
    @SuppressWarnings("SameReturnValue")
    public abstract @Nullable B recycle(@Nullable B batch);

    /**
     * Equivalent to {@link #recycle(Batch)}, but internally uses {@code threadId} as a
     * surrogate for {@code Thread.currentThread().threadId()}.
     *
     * @param threadId see {@link #createForThread(int, int)}
     * @param batch see {@link #recycle(Batch)}
     * @return see {@link #recycle(Batch)}
     */
    @SuppressWarnings("UnusedReturnValue")
    public abstract @Nullable B recycleForThread(int threadId, @Nullable B batch);

    /**
     * The preferred or default capacity of batches obtained from {@link #create(int)}, in
     * number of terms. The {@link Batch#termsCapacity()} of such batches is <strong>NOT</strong>
     * guaranteed to match this value.
     * @return preferred/target/default {@link Batch#termsCapacity()} for batches obtained via
     *         {@link #create(int)}
     */
    public short preferredTermsPerBatch() { return PREFERRED_BATCH_TERMS; }

    /**
     * Number rows equivalent to {@link #preferredTermsPerBatch()} for batches with the
     * given number of columns.
     *
     * @param columns number of columns to consider
     * @return {@code preferredTermsPerBatch()/max(1, columns)}.
     */
    public short preferredRowsPerBatch(int columns) {
        return (short)(preferredTermsPerBatch()/Math.max(1, columns));
    }

    /** Equivalent to {@link #preferredRowsPerBatch(int)} with {@code vars.size()}. */
    public short preferredRowsPerBatch(Vars vars) {
        return (short)(preferredTermsPerBatch()/Math.max(1, vars.size()));
    }

    /**
     * Create a {@link RowBucket} able to hold {@code rowsCapacity} rows.
     *
     * @param rowsCapacity maximum number of rows to be held in the bucket.
     * @param cols number of columns that all rows must have
     * @return a new {@link RowBucket}
     */
    public abstract RowBucket<B> createBucket(int rowsCapacity, int cols);

    /** Get a {@link BatchMerger} that only executes a projection on its left operand. */
    public abstract @Nullable BatchMerger<B> projector(Vars out, Vars in);

    /**
     * Get a merger that builds a new batch by merging columns of a fixed left-side row with all
     * rows in a right-side batch.
     *
     * <p>If {@code out.equals(left)}, this will return {@code null} as there is no work to
     * be done. If {@code leftVars.containsAll(out)}, this will be equivalent to
     * {@link BatchType#projector(Vars, Vars)}.</p>
     *
     * @param out   the variables of the result (merged) row
     * @param left  the variables present in {@code left} parameter of
     *              {@link BatchMerger#merge(Batch, Batch, int, Batch)}
     * @param right the variables present in {@code right} parameter of
     *              {@link BatchMerger#merge(Batch, Batch, int, Batch)}
     * @return a new {@link BatchMerger}
     */
    public abstract @NonNull BatchMerger<B> merger(Vars out, Vars left, Vars right);

    /**
     * Creates a {@link BatchFilter} that removes all rows for which
     * {@link RowFilter#drop(Batch, int)} returns {@code true} and applies a projection to
     * surviving rows (that must have {@code in.size()} columns) in order to output a
     * {@link Batch} whose columns correspond to {@code out}.
     *
     * @param out vars in the resulting {@link Batch}
     * @param in vars in the input batch ({@code in} in {@link BatchFilter#filterInPlace(Batch)}).
     * @param filter the filter that will be applied to each row
     * @param before a {@link BatchFilter} to always execute before this {@link BatchFilter}.
     * @return a {@link BatchFilter}
     */
    public abstract BatchFilter<B> filter(Vars out, Vars in, RowFilter<B> filter,
                                          BatchFilter<B> before);

    /** {@link #filter(Vars, Vars, RowFilter, BatchFilter)} with {@code before=null}. */
    public final BatchFilter<B> filter(Vars out, Vars in, RowFilter<B> filter) {
        return filter(out, in, filter, null);
    }

    /**
     * Creates a {@link BatchFilter} that removes all rows for which
     * {@link RowFilter#drop(Batch, int)} returns {@code true}.
     *
     * @param vars input <strong>AND</strong> output vars for batches filtered
     * @param filter the filter that will be applied to each row
     * @param before a {@link BatchFilter} to always execute before this {@link BatchFilter}.
     * @return a {@link BatchFilter}
     */
    public abstract BatchFilter<B> filter(Vars vars, RowFilter<B> filter, BatchFilter<B> before);

    /** {@link #filter(Vars, RowFilter, BatchFilter)} with {@code before=null} */
    public final BatchFilter<B> filter(Vars vars, RowFilter<B> filter) {
        return filter(vars, filter, null);
    }

    @Override public String toString() { return name; }

    @Override public boolean equals(Object o) {
        return o instanceof BatchType<?> && o.getClass().equals(getClass());
    }

    @Override public int hashCode() { return getClass().hashCode(); }
}
