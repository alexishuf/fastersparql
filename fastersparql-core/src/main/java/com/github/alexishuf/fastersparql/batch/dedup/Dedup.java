package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Integer.MAX_VALUE;

public abstract class Dedup<B extends Batch<B>, D extends Dedup<B, D>>
        extends AbstractOwned<D>
        implements RowFilter<B, D> {
    protected static final boolean DEBUG = Dedup.class.desiredAssertionStatus();
    protected final BatchType<B> bt;
    protected int cols;
    private final ReentrantLock lock = new ReentrantLock();

    public Dedup(BatchType<B> batchType, int cols) {
        this.bt = batchType;
        this.cols = cols;
    }

    public static <B extends Batch<B>>
    Orphan<WeakDedup<B>> weak(BatchType<B> batchType, int cols, DistinctType distinctType) {
        return new WeakDedup.Concrete<>(batchType, cols, distinctType);
    }

    public static <B extends Batch<B>>
    Orphan<WeakCrossSourceDedup<B>> weakCrossSource(BatchType<B> batchType, int cols) {
        return new WeakCrossSourceDedup.Concrete<>(batchType, cols);
    }

    public static <B extends Batch<B>>
    Orphan<StrongDedup<B>> strongUntil(BatchType<B> bt, int strongCapacity, int cols) {
        return new StrongDedup.Concrete<>(bt, Math.max(512, strongCapacity>>10), strongCapacity, cols);
    }

    public static <B extends Batch<B>>
    Orphan<StrongDedup<B>> strongForever(BatchType<B> bt, int initialCapacity, int cols) {
        return new StrongDedup.Concrete<>(bt, Math.max(512, initialCapacity), MAX_VALUE, cols);
    }

    protected void lock() {
        for (int i = 0; i < 32; ++i) {
            if (lock.tryLock())
                return;
            Thread.onSpinWait();
        }
        lock.lock();
    }

    protected void unlock() {
        lock.unlock();
    }

    protected static int enhanceHash(int hash) {
        return hash ^ (hash>>>24);
    }

    @Override public final void rebind(BatchBinding binding) throws RebindException {
        clear(cols);
    }

    protected void checkBatchType(B b) {
        if (DEBUG && b != null && !bt.batchClass().isAssignableFrom(b.getClass())) {
            throw new IllegalArgumentException("Unexpected batch of " + b.getClass() +
                                               ", expected " + bt.batchClass());
        }
    }

    public abstract int capacity();

    public final int cols() { return cols; }

    /**
     * Remove all rows from this {@link Dedup} and set it to receive rows with {@code cols} columns
     *
     * @param cols number of columns of subsequent rows to be added.
     */
    public abstract void clear(int cols);

    public final BatchType<B> batchType() { return bt; }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public abstract boolean isWeak();

    @Override public Decision drop(B batch, int row) {
        return isDuplicate(batch, row, 0) ? Decision.DROP : Decision.KEEP;
    }

    @Override public boolean targetsProjection() { return true; }

    /**
     * Create a {@link RowFilter} that calls {@link Dedup#isDuplicate(Batch, int, int)}
     * with given {@code sourceIdx}.
     */
    public Orphan<SourcedRowFilter<B>> sourcedFilter(int sourceIdx) {
        return new SourcedRowFilter.Concrete<>(this, sourceIdx);
    }

    public static abstract sealed class SourcedRowFilter<B extends Batch<B>>
            extends AbstractOwned<SourcedRowFilter<B>>
            implements RowFilter<B, SourcedRowFilter<B>> {
        public final Dedup<B, ?> dedup;
        public final int sourceIdx;
        protected SourcedRowFilter(Dedup<B, ?> dedup, int sourceIdx) {
            this.dedup = dedup;
            this.sourceIdx = sourceIdx;
        }
        private static final class Concrete<B extends Batch<B>>
                extends SourcedRowFilter<B>
                implements Orphan<SourcedRowFilter<B>> {
            private Concrete(Dedup<B, ?> dedup, int sourceIdx) {super(dedup, sourceIdx);}
            @Override public SourcedRowFilter<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }
        @Override public Decision drop(B b, int r) {
            return dedup.isDuplicate(b, r, sourceIdx) ? Decision.DROP : Decision.KEEP;
        }
        @Override public boolean targetsProjection() {return true;}
        @Override public void rebind(BatchBinding binding) {dedup.clear(dedup.cols);}

        @Override public @Nullable SourcedRowFilter<B> recycle(Object currentOwner) {
            return internalMarkRecycled(currentOwner);
        }
    }

    /**
     * Whether the given row, originated from the given source should NOT be considered a duplicate.
     *
     * @param batch batch containing a row
     * @param source index of the source within the set of sources. This will be ignored unless
     *               the implementation is doing cross-source deduplication, where only a row
     *               previously output by <strong>another</strong> source will be considered
     *               duplicate.
     * @return {@code true} iff {@code row} is a duplicate
     */
    public abstract boolean isDuplicate(B batch, int row, int source);

    /** What {@code isDuplicate(row, 0)} would return, but without storing {@code row}. */
    public abstract boolean contains(B batch, int row);

    /** Equivalent to {@code !isDuplicate(row, 0)}. */
    public final boolean add(B batch, int row) { return !isDuplicate(batch, row, 0); }

    /** Execute {@code consumer.accept(r)} for every batch in this set. */
    public abstract  <E extends Throwable> void forEach(ThrowingConsumer<B, E> consumer) throws E;
}
