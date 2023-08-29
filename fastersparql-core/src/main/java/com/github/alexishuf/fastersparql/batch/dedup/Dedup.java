package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;

import java.util.concurrent.locks.ReentrantLock;

public abstract class Dedup<B extends Batch<B>> extends ReentrantLock implements RowFilter<B> {
    protected static final boolean DEBUG = Dedup.class.desiredAssertionStatus();
    protected final BatchType<B> bt;
    protected int cols;
    protected short delayRelease;
    boolean released, pendingRelease;

    public Dedup(BatchType<B> batchType, int cols) {
        this.bt = batchType;
        this.cols = cols;
    }

    protected static int enhanceHash(int hash) {
        return hash ^ (hash>>>24);
    }

    @Override public final void rebind(BatchBinding<B> binding) throws RebindException {
        clear(cols);
    }

    protected void checkBatchType(B b) {
        if (!DEBUG || b == null || b.getClass() == bt.batchClass) return;
        throw new IllegalArgumentException("Unexpected batch of class "+b.getClass()+
                                          ", expected "+bt.batchClass);
    }

    public abstract int capacity();

    public final int cols() { return cols; }

    /**
     * Remove all rows from this {@link Dedup} and set it to receive rows with {@code cols} columns
     *
     * @param cols number of columns of subsequent rows to be added.
     */
    public abstract void clear(int cols);

    public abstract void recycleInternals();

    @Override public void release() {
        if (delayRelease > 0) {
            pendingRelease = true;
        } else if (!released) {
            released = true;
            recycleInternals();
        }
    }

    @Override public void rebindAcquire() {
        delayRelease++;
    }

    @Override public void rebindRelease() {
        if (--delayRelease <= 0 && pendingRelease) {
            released = true;
            recycleInternals();
        }
    }

    public final BatchType<B> batchType() { return bt; }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public abstract boolean isWeak();

    @Override public Decision drop(B batch, int row) {
        return isDuplicate(batch, row, 0) ? Decision.DROP : Decision.KEEP;
    }

    /**
     * Create a {@link RowFilter} that calls {@link Dedup#isDuplicate(Batch, int, int)}
     * with given {@code sourceIdx}.
     */
    public RowFilter<B> sourcedFilter(int sourceIdx) {
        return new RowFilter<>() {
            @Override public Decision drop(B b, int r) {
                return isDuplicate(b, r, sourceIdx) ? Decision.DROP : Decision.KEEP;
            }
            @Override public boolean targetsProjection() {return true;}
            @Override public void rebind(BatchBinding<B> binding) throws RebindException {
                clear(cols);
            }
        };
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

    @Override public String toString() {
        return String.format("%s@%x", getClass().getSimpleName(), System.identityHashCode(this));
    }
}
