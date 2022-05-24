package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowHashSet;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.plan.DistinctPlan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;

@RequiredArgsConstructor
public class InMemoryHashDistinct implements Distinct {
    private final RowOperations rowOps;

    public static class Provider implements DistinctProvider {
        @Override public @NonNegative int bid(long flags) {
            int bid = BidCosts.BUILTIN_COST;
            if ((flags & ASYNC)            != 0) bid += BidCosts.MINOR_COST;
            if ((flags & SPILLOVER)        != 0) bid += BidCosts.MINOR_COST;
            if ((flags & ALLOW_DUPLICATES) != 0) bid += BidCosts.MINOR_COST;
            bid += (flags & LARGE_FIRST) != 0 ? BidCosts.OOM_COST
                 : BidCosts.SLOW_COST * ((flags & SMALL_FIRST) != 0 ? 16 : 31);
            return bid;
        }

        @Override public Distinct create(long flags, RowOperations rowOperations) {
            return new InMemoryHashDistinct(rowOperations);
        }
    }

    @SuppressWarnings("unchecked") @Override public <R> Class<R> rowClass() {
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(DistinctPlan<R> plan) {
        Results<R> in = plan.input().execute();
        val p = new DistinctProcessor<>(in.publisher(), plan, new RowHashSet<>(rowOps));
        return new Results<>(in.vars(), in.rowClass(), p);
    }
}
