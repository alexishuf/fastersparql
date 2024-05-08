package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.operators.ConverterBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.Alloc;
import com.github.alexishuf.fastersparql.util.concurrent.Primer;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public abstract class BatchType<B extends Batch<B>> implements BatchConverter<B> {
    private static int bigQueryPoolSize() {
        int longJoinPath = 16;
        int manySources = 16;
        int queuedPerBindingStage = FSProperties.emitReqChunkBatches();
        return longJoinPath*manySources*queuedPerBindingStage;
    }
    protected static final int POOL_SHARED = Math.max(bigQueryPoolSize(), Alloc.THREADS*2048);
    /**
     * Preferred number of terms in the default size of batches obtained via
     * {@link #create(int)} and other related methods. {@link BatchType} implementations may
     * use other values, but using this values prevents situations where a single batch yields
     * more than one batch when converted to another type.
     */
    public static final short PREFERRED_BATCH_TERMS = 256;

    private static final int PRIME_ADD = bigQueryPoolSize();

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

    protected final Alloc<B> pool;
    /** A 0-based auto-increment id unique to this {@link BatchType} instance. */
    public final int id;
    private final String name;

    public BatchType(Class<B> cls, Supplier<B> primerFactory, Supplier<B> factory,
                     int bytesPerBatch) {
        this.id = (int)NEXT_ID.getAndAddRelease(1);
        if (this.id > MAX_BATCH_TYPE_ID)
            throw new IllegalStateException("Too many BatchType instances");
        this.name = getClass().getSimpleName().replaceAll("Type$", "");
        this.pool = new Alloc<>(cls, toString(), POOL_SHARED, factory, bytesPerBatch);
        Primer.INSTANCE.sched(() -> pool.prime(primerFactory, 2, PRIME_ADD));
    }

    /** Get the {@link Class} object of {@code B}. */
    @SuppressWarnings("unused") public final Class<B> batchClass() { return pool.itemClass(); }


    /**
     * Create or get a pooled batch that is empty and set for {@code cols} columns.
     *
     * @param cols desired {@link Batch#cols} of the returned batch
     * @return an empty {@link Batch}
     */
    public abstract Orphan<B> create(int cols);

    /**
     * Equivalent to {@link #create(int)}, but will return {@code null} instead of instantiating
     * a new {@link Batch} if the pool is empty.
     *
     * @param cols the desired {@link Batch#cols()}.
     * @return an orphan batch with {@code cols} columns or {@code null}.
     */
    public abstract @Nullable Orphan<B> poll(int cols);

    /**
     * Equivalent to {@link #create(int)} if {@code offer == null},
     * else return {@code offer} after {@link Batch#clear(int)}.
     *
     * @param offer possibly null batch to be used before trying the global pool.
     * @param owner current owner of {@code offer} (if {@code offer != null}) and owner of the
     *              batch returned by this call.
     * @param cols  number of columns in the batch returned by this method.
     * @return {@code offer}, cleared with {@code cols} columns or a new batch with space
     * to fit {@code rows} and {@code localBytes} bytes of local segments.
     */
    public abstract B empty(@Nullable B offer, Object owner, int cols);

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
    public abstract Orphan<B> createForThread(int threadId, int cols);

    /**
     * Equivalent to {@link #createForThread(int, int)}, but will not create a new batch instance:
     * If there is no batch in the pool, will return {@code null}.
     *
     * @param threadId see {@link #createForThread(int, int)}
     * @param cols see {@link #createForThread(int, int)}
     * @return an orphan batch with {@code cols} columns or null if there  was no pooled batch.
     */
    public abstract @Nullable Orphan<B> pollForThread(int threadId, int cols);

    /**
     * Equivalent to {@link #empty(Batch, Object, int)}, but will internally use {@code threadId} as a
     * surrogate for {@code Thread.currentThread().threadId()}.
     *
     * @param threadId see {@link #createForThread(int, int)}
     * @param offer see {@link #empty(Batch, Object, int)}
     * @param owner see {@link #empty(Batch, Object, int)}
     * @param cols see {@link #empty(Batch, Object, int)}
     * @return see {@link #empty(Batch, Object, int)}
     */
    public abstract B emptyForThread(int threadId, @Nullable B offer, Object owner, int cols);

    protected final B createForThread0(int threadId) {
        B b = pool.create(threadId);
        BatchEvent.Unpooled.record(b);
        return b;
    }

    protected final @Nullable B pollForThread0(int threadId) {
        B b = pool.poll(threadId);
        if (b == null)
            return null;
        BatchEvent.Unpooled.record(b);
        return b;
    }

    protected final B emptyForThread0(int threadId, @Nullable B offer, Object owner) {
        if (offer == null) {
            offer = pool.create(threadId);
            BatchEvent.Unpooled.record(offer);
            offer.releaseOwnership(RECYCLED).takeOwnership(owner);
        } else {
            offer.requireOwner(owner);
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

    public <I extends Batch<I>> Orphan<? extends Emitter<B, ?>>
    convert(Orphan<? extends Emitter<I, ?>> emitter) {
        if (equals(Emitter.peekBatchTypeWild(emitter))) //noinspection unchecked
            return (Orphan<? extends Emitter<B, ?>>)emitter;
        return ConverterStage.create(this, emitter);
    }

    /**
     * Convert/copy a single batch.
     *
     * @param src original batch
     * @return a batch of this type ({@code B}) with the same rows as {@code src}
     */
    public <I extends Batch<I>> Orphan<B> convertOrCopy(I src) {
        B b = create(src.cols).takeOwnership(this);
        b.putConverting(src);
        return b.releaseOwnership(this);
    }

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
    public abstract Orphan<? extends RowBucket<B, ?>>
    createBucket(int rowsCapacity, int cols);

    /** Estimate how many bytes of RAM a {@link RowBucket} with the requested capacity will use. */
    public abstract int bucketBytesCost(int rowsCapacity, int cols);

    /** Get a {@link BatchMerger} that only executes a projection on its left operand. */
    public abstract @Nullable Orphan<? extends BatchMerger<B, ?>>
    projector(Vars out, Vars in);

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
     *              {@link BatchMerger#merge(Orphan, Batch, int, Batch)}
     * @param right the variables present in {@code right} parameter of
     *              {@link BatchMerger#merge(Orphan, Batch, int, Batch)}
     * @return a new {@link BatchMerger}
     */
    public abstract @NonNull Orphan<? extends BatchMerger<B, ?>>
    merger(Vars out, Vars left, Vars right);

    /**
     * Creates a {@link BatchFilter} that removes all rows for which
     * {@link RowFilter#drop(Batch, int)} returns {@code true} and applies a projection to
     * surviving rows (that must have {@code in.size()} columns) in order to output a
     * {@link Batch} whose columns correspond to {@code out}.
     *
     * @param out vars in the resulting {@link Batch}
     * @param in vars in the input batch ({@code in} in {@link BatchFilter#filterInPlace(Orphan)}).
     * @param filter the filter that will be applied to each row
     * @param before a {@link BatchFilter} to always execute before this {@link BatchFilter}.
     * @return a {@link BatchFilter}
     */
    public abstract Orphan<? extends BatchFilter<B, ?>> filter(Vars out, Vars in,
                                          Orphan<? extends RowFilter<B, ?>> filter,
                                          Orphan<? extends BatchFilter<B, ?>> before);

    /** {@link #filter(Vars, Vars, Orphan, Orphan)} with {@code before=null}. */
    public final Orphan<? extends BatchFilter<B, ?>> filter(Vars out, Vars in, Orphan<? extends RowFilter<B, ?>> filter) {
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
    public abstract Orphan<? extends BatchFilter<B, ?>> filter(Vars vars,
                                          Orphan<? extends RowFilter<B, ?>> filter,
                                          Orphan<? extends BatchFilter<B, ?>> before);

    /** {@link #filter(Vars, Orphan, Orphan)} with {@code before=null} */
    public final Orphan<? extends BatchFilter<B, ?>>
    filter(Vars vars, Orphan<? extends RowFilter<B, ?>> filter) {
        return filter(vars, filter, null);
    }

    @Override public String toString() { return name; }

    @Override public boolean equals(Object o) {
        return o instanceof BatchType<?> && o.getClass().equals(getClass());
    }

    @Override public int hashCode() { return getClass().hashCode(); }
}
