package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Minus;
import com.github.alexishuf.fastersparql.operators.impl.Merger;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.MinusProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.List;

@Slf4j
@Value @Accessors(fluent = true)
public class BindMinus implements Minus {
    RowOperations rowOps;
    int bindConcurrency;

    public static class Provider implements MinusProvider {
        @Override public @NonNegative int bid(long flags) {
            return BindJoin.Provider.bindCost(flags);
        }
        @Override public Minus create(long flags, RowOperations rowOperations) {
            return new BindMinus(rowOperations, BindJoin.Provider.concurrency(flags));
        }
    }

    @Override public <R> Results<R> checkedRun(Plan<R> left, Plan<R> right) {
        List<String> leftVars = left.publicVars();
        Merger<R> merger = new Merger<>(rowOps, leftVars, right, leftVars);
        if (merger.isProduct()) {
            log.trace("Returning left Minus as operands share no var left={}, right={}",
                      left, right);
            return left.execute();
        }
        Results<R> lr = left.execute();
        BindJoinPublisher<R> pub = new BindJoinPublisher<>(bindConcurrency, lr.publisher(), merger,
                                                           BindJoinPublisher.JoinType.MINUS);
        return new Results<>(leftVars, lr.rowClass(), pub);
    }
}
