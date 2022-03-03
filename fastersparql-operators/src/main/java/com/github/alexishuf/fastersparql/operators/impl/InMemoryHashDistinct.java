package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.DistinctPlan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.AllArgsConstructor;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;

import java.util.HashSet;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;
import static java.lang.System.nanoTime;
import static java.util.function.Function.identity;

public class InMemoryHashDistinct implements Distinct {
    private final Function<Object, Object> wrap;

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

    public InMemoryHashDistinct(RowOperations rowOps) {
        this.wrap = rowOps.needsCustomHash() ? r -> new HashAdapter(rowOps, r) : identity();
    }

    @Override public <R> Results<R> checkedRun(DistinctPlan<R> plan) {
        Results<R> in = plan.input().execute();
        val hasher = new Hasher<>(in.publisher(), wrap, plan, in.rowClass());
        return new Results<>(in.vars(), in.rowClass(), hasher);
    }

    @AllArgsConstructor
    static class HashAdapter {
        private final RowOperations ops;
        private final Object val;

        @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
        @Override public boolean equals(Object o) { return ops.equalsSameVars(val, o); }
        @Override public int hashCode() { return ops.hash(val); }
        @Override public String toString() { return val.toString(); }
    }

    private static class Hasher<R> extends AbstractProcessor<R, R> {
        private final DistinctPlan<R> distinctPlan;
        private final Class<? super R> rowClass;
        private final HashSet<Object> set = new HashSet<>();
        private final Function<Object, ?> wrap;

        public Hasher(Publisher<? extends R> source, Function<Object, ?> wrap,
                      DistinctPlan<R> distinctPlan, Class<? super R> rowClass) {
            super(source);
            this.wrap = wrap;
            this.distinctPlan = distinctPlan;
            this.rowClass = rowClass;
        }

        @Override protected void handleOnNext(R item) {
            if (set.add(wrap.apply(item)))
                emit(item);
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
            if (hasGlobalMetricsListeners()) {
                sendMetrics(new PlanMetrics<>(distinctPlan, rowClass, rows,
                                              start, nanoTime(), error, cancelled));
            }
        }
    }
}
