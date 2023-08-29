package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;

public interface RowBucket<B extends Batch<B>> extends Iterable<B> {
    @SuppressWarnings("unused") BatchType<B> batchType();

    /**
     * Increase capacity to {@link RowBucket#capacity()}+{@code additionalRows}.
     *
     * @param additionalRows number of additional rows that must fit
     */
    void grow(int additionalRows);

    /**
     * Remove all rows stored in this bucket configures it to store rows with {@code cols} columns
     * and, if required, grow storage so that {@code capacity() >= rowsCapacity}.
     *
     * @param rowsCapacity required capacity after clear
     * @param cols new number of columns in future rows.
     */
    void clear(int rowsCapacity, int cols);

    /**
     * Releases internal resources which are pooled. This MAY change {@link #capacity()} and
     * {@link #cols()} MAY clear totally or partially the contents of this bucket.
     *
     * <p>A subsequent {@link #clear(int, int)} call will restore the bucket to a determined
     * capacity and will reset all rows.</p>
     */
    void recycleInternals();

    /** How many rows fit in this bucket. */
    int capacity();

    /** Number of columns for rows in this bucket. */
    int cols();

    /** Whether there is a row stored at slot {@code row}. */
    boolean has(int row);

    /** Copy {@code row}-th row of {@code batch} into slot {@code dst} */
    void set(int dst, B batch, int row);

    /** Copy row at {@code src} into {@code dst} */
    void set(int dst, int src);

    /**
     * Copy row {@code row} of {@code other} into row {@code dst} of {@code this} {@link RowBucket}.
     */
    void set(int dst, RowBucket<B> other, int src);

    /**
     * Whether the row stored at slot {@code row} is equal to the {@code batchRow}-th row of
     * {@code batch}
     */
    boolean equals(int row, B other, int otherRow);

    /**
     * Hash code for the {@code row}-th row in this bucket. If {{@link #has(int)}}, the
     * hash code MUST be the same as {@link Batch#hash(int)} would return.
     */
    int hashCode(int row);

    /** Create a string representation of this bucket. */
    @SuppressWarnings("unused") default ByteRope dump() {
        var br = new ByteRope();
        br.append('[');
        int rows = capacity();
        for (int r = 0; r < rows; r++)
            dump(br.append('\n').append(' '), r);
        return br.append('\n').append(']');
    }

    /** Append a representation of the {@code row}-th row of this bucket to {@code dest} */
    void dump(ByteRope dest, int row);
}
