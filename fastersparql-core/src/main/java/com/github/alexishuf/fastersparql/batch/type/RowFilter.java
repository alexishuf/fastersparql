package com.github.alexishuf.fastersparql.batch.type;

public interface RowFilter<B extends Batch<B>> {
    enum Decision {
        /** Do not drop the evaluated row. */
        KEEP,
        /** Drop the evaluated row. */
        DROP,
        /**
         * Drop the evaluated row, all subsequent rows in the current batch and
         * all rows in future batches.
         */
        TERMINATE
    }

    Decision drop(B batch, int row);

    /**
     * Whether {@link RowFilter#drop(Batch, int)} must be evaluated on the projected
     * batch rather than the input batch if there is a projection being executed
     * concurrently with filtering.
     */
    default boolean targetsProjection() { return false; }

    /**
     * Clears any state that is mutated by calls to {@link #drop(Batch, int)}, reverting to the
     * state this filter was when constructuted (before first {@code drop()} call);
     */
    default void reset() {}
}
