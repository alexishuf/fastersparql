package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties;
import com.github.alexishuf.fastersparql.operators.Join;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.impl.Merger;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.JoinProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import java.util.List;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOpProperties.bindJoinReorder;
import static com.github.alexishuf.fastersparql.operators.JoinHelpers.executeReorderedLeftAssociative;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.LARGE_FIRST;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.SMALL_SECOND;

@Value @Accessors(fluent = true)
public class BindJoin implements Join {
    RowOperations rowOps;
    @Positive int bindConcurrency;

    public static class Provider implements JoinProvider {
        static @NonNegative int bindCost(long flags) {
            int cost = BidCosts.BUILTIN_COST;
            if ((flags & SMALL_SECOND) != 0 && (flags & LARGE_FIRST) != 0)
                cost += 2*BidCosts.SLOW_COST;
            return cost;
        }
        static @Positive int concurrency(long flags) {
            return (flags & OperatorFlags.ASYNC) == 0
                    ? 1 : FasterSparqlOpProperties.bindConcurrency();
        }

        @Override public @NonNegative int bid(long flags) {
            return bindCost(flags);
        }

        @Override public Join create(long flags, RowOperations rowOperations) {
            return new BindJoin(rowOperations, concurrency(flags));
        }
    }

    @Override public <R> Results<R> checkedRun(List<? extends Plan<R>> unorderedOperands) {
        return executeReorderedLeftAssociative(unorderedOperands, bindJoinReorder(),
                                               true, this::execute);
    }

    private <R> Results<R> execute(Results<R> left, Plan<R> right) {
        Merger<R> merger = new Merger<>(rowOps, left.vars(), right);
        return new Results<>(merger.outVars(), left.rowClass(),
                new BindJoinPublisher<>(bindConcurrency, left.publisher(), merger,
                                         BindJoinPublisher.JoinType.INNER));
    }
}
