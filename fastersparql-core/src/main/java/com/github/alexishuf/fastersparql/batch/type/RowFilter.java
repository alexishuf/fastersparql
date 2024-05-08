package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.emit.Rebindable;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Owned;

public interface RowFilter<B extends Batch<B>, R extends RowFilter<B, R>>
        extends Rebindable, Owned<R> {
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

    default boolean isNoOp() { return false; }

    default @Override Vars bindableVars() { return Vars.EMPTY; }
}
