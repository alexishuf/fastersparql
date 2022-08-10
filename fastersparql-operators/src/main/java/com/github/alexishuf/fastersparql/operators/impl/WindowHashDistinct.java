package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowHashWindowSet;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.plan.DistinctPlan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import org.checkerframework.checker.index.qual.NonNegative;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;

public final class WindowHashDistinct implements Distinct {
    private final RowOperations rowOps;
    private final int overrideWindow;

    public static class Provider implements DistinctProvider {
        @Override public @NonNegative int bid(long flags) {
            if ((flags & ALLOW_DUPLICATES) == 0)
                return BidCosts.UNSUPPORTED;
            return BidCosts.BUILTIN_COST + 8 * BidCosts.SLOW_COST
                    + (((flags &     ASYNC) != 0) ? BidCosts.MINOR_COST : 0)
                    + (((flags & SPILLOVER) != 0) ? BidCosts.MINOR_COST : 0);
        }

        @Override public Distinct create(long flags, RowOperations rowOperations) {
            return new WindowHashDistinct(rowOperations, -1);
        }
    }

    public WindowHashDistinct(RowOperations rowOps, int overrideWindow) {
        this.rowOps = rowOps;
        this.overrideWindow = overrideWindow;
    }

    @SuppressWarnings("unchecked") @Override public <R> Class<R> rowClass() {
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(DistinctPlan<R> plan) {
        Results<R> in = plan.input().execute();
        int window = overrideWindow >= 0 ? overrideWindow
                                         : FasterSparqlOpProperties.distinctWindow();
        RowHashWindowSet<R> set = new RowHashWindowSet<>(window, rowOps);
        DistinctProcessor<R> processor = new DistinctProcessor<>(in.publisher(), plan, set);
        return new Results<>(in.vars(), in.rowClass(), processor);
    }
}
