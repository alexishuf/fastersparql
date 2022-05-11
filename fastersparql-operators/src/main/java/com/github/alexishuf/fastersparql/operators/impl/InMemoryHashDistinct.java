package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.DistinctPlan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.val;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashSet;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;
import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;
import static java.lang.System.nanoTime;
import static java.util.function.Function.identity;

@Accessors(fluent = true)
public class InMemoryHashDistinct implements Distinct {
    @Getter private final Class<?> rowClass;
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
        this.rowClass = rowOps.rowClass();
        this.wrap = rowOps.needsCustomHash() ? r -> new HashAdapter(rowOps, r) : identity();
    }

    @Override public <R> Results<R> checkedRun(DistinctPlan<R> plan) {
        Results<R> in = plan.input().execute();
        val hasher = new Hasher<>(in.publisher(), wrap, plan);
        return new Results<>(in.vars(), in.rowClass(), hasher);
    }

    @AllArgsConstructor
    static class HashAdapter {
        private final RowOperations ops;
        private final Object val;

        @Override public boolean equals(Object o) {
            if (o instanceof HashAdapter)
                return ops.equalsSameVars(val, ((HashAdapter) o).val);
            return false;
        }
        @Override public int hashCode() { return ops.hash(val); }
        @Override public String toString() { return val.toString(); }
    }

    private static class Hasher<R> extends AbstractProcessor<R, R> {
        private final DistinctPlan<R> plan;
        private final HashSet<Object> set = new HashSet<>();
        private final Function<Object, ?> wrap;

        public Hasher(FSPublisher<? extends R> src, Function<Object, ?> wrap, DistinctPlan<R> plan) {
            super(src);
            this.wrap = wrap;
            this.plan = plan;
        }

        @Override protected void handleOnNext(R item) {
            if (set.add(wrap.apply(item)))
                emit(item);
        }

        @Override protected void onTerminate(@Nullable Throwable error, boolean cancel) {
            if (hasGlobalMetricsListeners()) {
                val metrics = new PlanMetrics(plan.name(), rows, start, nanoTime(), error, cancel);
                sendMetrics(plan, metrics);
            }
        }
    }
}
