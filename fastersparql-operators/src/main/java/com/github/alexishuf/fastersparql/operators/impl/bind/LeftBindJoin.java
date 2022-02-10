package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.LeftJoin;
import com.github.alexishuf.fastersparql.operators.impl.Merger;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.LeftJoinProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;

@Value @Accessors(fluent = true)
public class LeftBindJoin implements LeftJoin {
    RowOperations rowOps;
    int bindConcurrency;

    public static class Provider implements LeftJoinProvider {
        @Override public @NonNegative int bid(long flags) {
            return BindJoin.Provider.bindCost(flags);
        }
        @Override public LeftJoin create(long flags, RowOperations rowOperations) {
            return new LeftBindJoin(rowOperations, BindJoin.Provider.concurrency(flags));
        }
    }

    @Override public <R> Results<R> checkedRun(Plan<R> left, Plan<R> right) {
        Merger<R> merger = new Merger<>(rowOps, left.publicVars(), right);
        Results<R> lr = left.execute();
        BindJoinPublisher<R> pub = new BindJoinPublisher<>(bindConcurrency, lr.publisher(), merger,
                                                           BindJoinPublisher.JoinType.LEFT);
        return new Results<>(merger.outVars(), lr.rowClass(), pub);
    }
}
