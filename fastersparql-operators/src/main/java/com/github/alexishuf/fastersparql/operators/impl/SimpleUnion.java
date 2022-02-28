package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Union;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.PlanHelpers;
import com.github.alexishuf.fastersparql.operators.providers.UnionProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher.concurrent;
import static com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher.eager;

@Value
public class SimpleUnion  implements Union {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    RowOperations rowOps;
    boolean parallel;

    public static class Provider implements UnionProvider {
        @Override public @NonNegative int bid(long flags) {
            return BidCosts.BUILTIN_COST;
        }

        @Override public Union create(long flags, RowOperations rowOperations) {
            boolean parallelSubscribe = (flags & OperatorFlags.ASYNC) != 0;
            return new SimpleUnion(rowOperations, parallelSubscribe);
        }
    }

    @Override public <R> Results<R> checkedRun(List<? extends Plan<R>> plans) {
        List<String> unionVars = PlanHelpers.publicVarsUnion(plans);
        String name = "SimpleUnion-"+nextId.getAndIncrement();
        MergePublisher<R> merge = (parallel ? eager().name(name) : concurrent(plans.size()))
                .name(name).build();
        Class<? super R> rCls = Object.class;
        for (Plan<R> p : plans) {
            Results<R> results = p.execute();
            if (rCls.equals(Object.class)) rCls = results.rowClass();
            else
                assert results.rowClass().equals(Object.class) || rCls.equals(results.rowClass());
            merge.addPublisher(new ProjectingProcessor<>(results, unionVars, rowOps));
        }
        merge.markCompletable();
        return new Results<>(unionVars, rCls, merge);
    }
}
