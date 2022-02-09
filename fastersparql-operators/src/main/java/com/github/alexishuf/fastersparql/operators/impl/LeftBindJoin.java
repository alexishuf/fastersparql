package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.LeftJoin;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.LeftJoinProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.List;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.LARGE_FIRST;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.SMALL_SECOND;

@Value @Accessors(fluent = true)
public class LeftBindJoin implements LeftJoin {
    RowOperations rowOps;
    int bindConcurrency;

    public static class Provider implements LeftJoinProvider {
        @Override public @NonNegative int bid(long flags) {
            int cost = BidCosts.BUILTIN_COST;
            if ((flags & SMALL_SECOND) != 0 && (flags & LARGE_FIRST) != 0)
                cost += 2*BidCosts.SLOW_COST;
            return cost;
        }

        @Override public LeftJoin create(long flags, RowOperations rowOperations) {
            int bindConcurrency = (flags & OperatorFlags.ASYNC) == 0 ? 1
                                : FasterSparqlOpProperties.bindConcurrency();
            return new LeftBindJoin(rowOperations, bindConcurrency);
        }
    }

    @Override public <R> Results<R> checkedRun(Plan<R> left, Plan<R> right) {
        List<String> lv = left.publicVars(), outVars = VarUtils.union(lv, right.publicVars());
        Merger<R> merger = new Merger<>(rowOps, lv, right, outVars);
        Results<R> lr = left.execute();
        BindJoinPublisher<R> pub = new BindJoinPublisher<>(bindConcurrency, lr.publisher(), merger,
                                                           BindJoinPublisher.JoinType.LEFT);
        return new Results<>(outVars, lr.rowClass(), pub);
    }
}
