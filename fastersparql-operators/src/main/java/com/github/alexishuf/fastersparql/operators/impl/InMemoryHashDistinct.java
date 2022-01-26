package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.providers.DistinctProvider;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import lombok.AllArgsConstructor;
import org.checkerframework.checker.index.qual.NonNegative;
import org.reactivestreams.Publisher;

import java.util.HashSet;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.operators.OperatorFlags.*;
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

    @Override public <R> Results<R> checkedRun(Plan<R> inputPlan) {
        Results<R> in = inputPlan.execute();
        return new Results<>(in.vars(), in.rowClass(), new Hasher<>(in.publisher(), wrap));
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
        private final HashSet<Object> set = new HashSet<>();
        private final Function<Object, ?> wrap;

        public Hasher(Publisher<? extends R> source, Function<Object, ?> wrap) {
            super(source);
            this.wrap = wrap;
        }

        @Override protected void handleOnNext(R item) {
            if (set.add(wrap.apply(item)))
                emit(item);
        }
    }
}
