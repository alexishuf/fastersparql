package com.github.alexishuf.fastersparql.batch.dedup;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.ProjectionRowFilter;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.util.ThrowingConsumer;

public abstract class Dedup<B extends Batch<B>> extends ProjectionRowFilter<B> {
    public abstract int capacity();

    /**
     * Remove all rows from this {@link Dedup} and set it to receive rows with {@code cols} columns
     *
     * @param cols number of columns of subsequent rows to be added.
     */
    public abstract void clear(int cols);

    public abstract BatchType<B> batchType();

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public abstract boolean isWeak();

    @Override public boolean drop(B batch, int row) { return isDuplicate(batch, row, 0); }

    /**
     * Create a {@link RowFilter} that calls {@link Dedup#isDuplicate(Batch, int, int)}
     * with given {@code sourceIdx}.
     */
    public RowFilter<B> sourcedFilter(int sourceIdx) {
        return new ProjectionRowFilter<>() {
            @Override public boolean drop(B b, int r) { return isDuplicate(b, r, sourceIdx); }
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
}
