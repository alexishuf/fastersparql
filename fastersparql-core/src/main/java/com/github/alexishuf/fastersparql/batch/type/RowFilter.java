package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.emit.Rebindable;
import com.github.alexishuf.fastersparql.model.Vars;

public interface RowFilter<B extends Batch<B>> extends Rebindable {
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

    default long upstreamRequestLimit() { return Long.MAX_VALUE; }

    /**
     * Whether {@link RowFilter#drop(Batch, int)} must be evaluated on the projected
     * batch rather than the input batch if there is a projection being executed
     * concurrently with filtering.
     */
    default boolean targetsProjection() { return false; }

    /**
     * Notifies that this {@link RowFilter} will not receive any subsequent calls and it
     * should release resources it holds.
     */
    default void release() { }

    @Override default void rebindAcquire() {
        // Nearly all RowFilter implementations hold no resources or are extremely heavy and
        // thus require pooling (Dedup). For the former, this and rebindRelease() can safely
        // be a no-op. For the latter, the pooled object cannot pool itself due to the risk
        // that other object keeps a reference, which can lead to 2+ concurrent random users
        // of the object.
    }

    @Override default void rebindRelease() {}

    default @Override Vars bindableVars() { return Vars.EMPTY; }
}
