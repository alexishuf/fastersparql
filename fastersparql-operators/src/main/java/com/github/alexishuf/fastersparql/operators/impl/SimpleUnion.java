package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.MergePublisher;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.Union;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.PlanHelpers;
import com.github.alexishuf.fastersparql.operators.plan.UnionPlan;
import com.github.alexishuf.fastersparql.operators.providers.UnionProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;

@Value @Accessors(fluent = true)
public class SimpleUnion  implements Union {
    Class<?> rowClass;
    RowOperations rowOps;
    boolean parallel;

    public static class Provider implements UnionProvider {
        @Override public @NonNegative int bid(long flags) {
            return BidCosts.BUILTIN_COST;
        }

        @Override public Union create(long flags, RowOperations ro) {
            boolean parallelSubscribe = (flags & OperatorFlags.ASYNC) != 0;
            return new SimpleUnion(ro.rowClass(), ro, parallelSubscribe);
        }
    }

    @Override public <R> Results<R> checkedRun(UnionPlan<R> plan) {
        List<? extends Plan<R>> plans = plan.inputs();
        List<String> unionVars = PlanHelpers.publicVarsUnion(plans);
        UnionPublisher<R> merge = new UnionPublisher<>(parallel ? 1 : plans.size(), plan);
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

    private static final class UnionPublisher<R> extends MergePublisher<R> {
        private static final AtomicInteger nextId = new AtomicInteger(1);
        private final UnionPlan<R> plan;
        private long start = Long.MAX_VALUE, rows = 0;

        public UnionPublisher(int concurrency, UnionPlan<R> plan) {
            super(plan.name()+"["+nextId.getAndIncrement()+"]",
                  concurrency, concurrency, false, null);
            this.plan = plan;
        }

        @Override public void subscribe(Subscriber<? super R> s) {
            start = System.nanoTime();
            super.subscribe(s);
        }

        @Override protected void feed(R item) {
            super.feed(item);
            ++rows;
        }

        @Override protected void onComplete(Throwable cause, boolean cancelled) {
            if (hasGlobalMetricsListeners()) {
                sendMetrics(plan, new PlanMetrics(plan.name(), rows, start, System.nanoTime(),
                                                  cause, cancelled));
            }
        }
    }
}
