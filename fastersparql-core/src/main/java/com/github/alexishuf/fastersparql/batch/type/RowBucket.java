package com.github.alexishuf.fastersparql.batch.type;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface RowBucket<B extends Batch<B>> extends Iterable<B> {
    BatchType<B> batchType();

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
    default void recycleInternals() {}

    /** How many rows fit in this bucket. */
    int capacity();

    /** Number of columns for rows in this bucket. */
    int cols();

    /** Whether there is a row stored at slot {@code row}. */
    boolean has(int row);

    /**
     * The underlying batch that where slot {@code rowSlot} of this bucket is stored.
     *
     * @param rowSlot a slot index in this bucket
     * @return a batch containing the row at {@code rowSlot} in some index, or null.
     * @see RowBucket#batchRow(int)
     */
    @Nullable B batchOf(int rowSlot);

    /**
     * Get the index into {@code batchOf(rowSlot}} corresponding to the bucket slot {@code rowSlot}.
     *
     * @param rowSlot a slot index in this bucket
     * @return {@code i} such that {@code equals(rowSlot, batchOf(rowSlot), i)}.
     */
    int batchRow(int rowSlot);

    /** Copy {@code row}-th row of {@code batch} into slot {@code dst} */
    void set(int dst, B batch, int row);

    /** Copy row at {@code src} into {@code dst} */
    void set(int dst, int src);

    /**
     * Whether the row stored at slot {@code row} is equal to the {@code batchRow}-th row of
     * {@code batch}
     */
    boolean equals(int row, B other, int otherRow);
}
