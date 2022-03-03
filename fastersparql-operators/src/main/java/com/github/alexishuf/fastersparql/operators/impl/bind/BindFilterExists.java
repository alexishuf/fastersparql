package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.FilterExists;
import com.github.alexishuf.fastersparql.operators.impl.Merger;
import com.github.alexishuf.fastersparql.operators.impl.bind.BindJoinPublisher.JoinType;
import com.github.alexishuf.fastersparql.operators.plan.FilterExistsPlan;
import com.github.alexishuf.fastersparql.operators.providers.FilterExistsProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

@Value  @Accessors(fluent = true)
public class BindFilterExists implements FilterExists {
    RowOperations rowOps;
    @Positive int bindConcurrency;

    public static class Provider implements FilterExistsProvider {

        @Override public @NonNegative int bid(long flags) {
            return BidCosts.BUILTIN_COST;
        }

        @Override public FilterExists create(long flags, RowOperations rowOperations) {
            return new BindFilterExists(rowOperations, BindJoin.Provider.concurrency(flags));
        }
    }

    @Override public <R> Results<R> checkedRun(FilterExistsPlan<R> plan) {
        Results<R> l = plan.input().execute();
        Merger<R> merger = new Merger<>(rowOps, l.vars(), plan.filter(), l.vars());
        JoinType joinType = plan.negate() ? JoinType.FILTER_NOT_EXISTS : JoinType.FILTER_EXISTS;
        return new Results<>(l.vars(), l.rowClass(),
                new BindJoinPublisher<>(bindConcurrency, l.publisher(), merger, joinType, plan));
    }
}
