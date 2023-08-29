package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.ConverterBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.AffinityLevelPool;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public abstract class BatchType<B extends Batch<B>> implements BatchConverter<B> {
    private static final int   TINY_LEVEL_CAP =   64;
    private static final int  SMALL_LEVEL_CAP =  128;
    private static final int MEDIUM_LEVEL_CAP =  128;
    private static final int  LARGE_LEVEL_CAP =  128;
    private static final int   HUGE_LEVEL_CAP =   64;
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

    public final Class<B> batchClass;
    /** A 0-based auto-increment id unique to this {@link BatchType} instance. */
    public final int id;
    private final String name;
    protected final AffinityLevelPool<B> pool;

    protected BatchType(Class<B> batchClass) {
        this.id = (int)NEXT_ID.getAndAddRelease(1);
        if (this.id > MAX_BATCH_TYPE_ID)
            throw new IllegalStateException("Too many BatchType instances");
        this.batchClass = batchClass;
        this.name = getClass().getSimpleName().replaceAll("Type$", "");
        this.pool = new AffinityLevelPool<>(new LevelPool<>(batchClass, TINY_LEVEL_CAP,
                SMALL_LEVEL_CAP, MEDIUM_LEVEL_CAP, LARGE_LEVEL_CAP, HUGE_LEVEL_CAP));
    }

    /** Get the {@link Class} object of {@code B}. */
    @SuppressWarnings("unused") public final Class<B> batchClass() { return batchClass; }

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
     * Create an empty {@link Batch} with given initial capacity.
     *
     * @param rowsCapacity number of rows that the batch may hold. offer methods
     *                     (see {@link Batch#beginOffer()}) are still allowed to reject
     *                     insertions before this is reached. Some implementations may
     *                     ignore this in favor of {@code bytesCapacity}
     * @param cols number of columns in the batch
     * @param localBytes number of bytesCapacity to allocate. Some implementations may ignore this in favor
     *                   of {@code rowsCapacity}
     * @return an empty {@link Batch}
     */
    public abstract B create(int rowsCapacity, int cols, int localBytes);

    /**
     * Equivalent to {@link #create(int, int, int)} if {@code offer == null},
     * else return {@code offer} after {@link Batch#clear(int)}.
     *
     * @param offer possibly null batch to be used before trying the global pool.
     * @param rows if offer is {@code null}, how many rows should fit in the new batch.
     * @param cols number of columns in the batch returned by this method.
     * @param localBytes if {@code offer is null}, how many local segment bytes should
     *                   fit in the new batch.
     * @return {@code offer}, cleared with {@code cols} columns or a new batch with space
     *         to fit {@code rows} and {@code localBytes} bytes of local segments.
     */
    public B empty(@Nullable B offer, int rows, int cols, int localBytes) {
        if (offer == null)
            return create(rows, cols, localBytes);
        offer.clear(cols);
        return offer;
    }

    /**
     * Analogous to {@link #empty(Batch, int, int, int)} but calls {@link Batch#reserve(int, int)}
     * on {@code offer}, if not null, to ensure it can fit the required {@code rows} and
     * {@code localBytes}.
     */
    public B reserved(@Nullable B offer, int rows, int cols, int localBytes) {
        if (offer == null)
            offer = create(rows, cols, localBytes);
        else
            offer.clear(cols);
        offer.reserve(rows, localBytes);
        return offer;
    }

    /**
     * Create a batch that will very likely hold at most single row.
     *
     * @param cols number of columns for the new batch
     * @return a new empty batch that can hold @{code cols} columns.
     */
    public B createSingleton(int cols) { return create(1, cols, 0); }

    /**
     * What should be {@code bytesCapacity} for a {@link BatchType#create(int, int, int)} call
     * whose resulting batch will be target of a {@link Batch#putConverting(Batch)}.
     *
     * <p>Example usage:</p>
     *
     * <pre>{@code
     *   B converted = type.create(b.rows, b.cols, type.bytesCapacity(b))
     *   converted.putConverting(b)
     * }</pre>
     *
     * @param b a batch for which bytes usage of local segments will be computed
     * @return the required number of bytes
     */
    public int localBytesRequired(Batch<?> b) { return b.localBytesUsed(); }

    /**
     * Convert/copy a single batch.
     *
     * @param src original batch
     * @return a batch of this type ({@code B}) with the same rows as {@code src}
     */
    public final <O extends Batch<O>> B convert(O src) {
        return create(src.rows, src.cols, localBytesRequired(src)).putConverting(src);
    }

    /**
     * Offer a batch for recycling so that it may be returned in future
     * {@link BatchType#create(int, int, int)}/{@link BatchType#createSingleton(int)} calls.
     *
     * @param batch the {@link Batch} to be recycled
     * @return {@code null}, for conveniently setting recycled references to null:
     *         {@code b = type.recycle(b)}
     */
    @SuppressWarnings("SameReturnValue") public abstract @Nullable B recycle(@Nullable B batch);

    /**
     * Create a {@link RowBucket} able to hold {@code rowsCapacity} rows.
     *
     * @param rowsCapacity maximum number of rows to be held in the bucket.
     * @param cols number of columns that all rows must have
     * @return a new {@link RowBucket}
     */
    public abstract RowBucket<B> createBucket(int rowsCapacity, int cols);

    /**
     * Offer a {@link RowBucket} instance to be returned in a future
     * {@link #createBucket(int, int)} call.
     *
     * @param bucket the {@link RowBucket} to be offered, the caller must have exclusive ownership when calling and must not access {@code bucket} after this call.
     * @return {@code null}, allowing {@code bucket = bt.recycleBucket(bucket)}
     */
    @SuppressWarnings("SameReturnValue") public @Nullable RowBucket<B> recycleBucket(RowBucket<B> bucket) {
        bucket.recycleInternals();
        return null;
    }

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
     * @param in vars in the input batch ({@code in} in {@link BatchFilter#filter(Batch, Batch)}).
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
