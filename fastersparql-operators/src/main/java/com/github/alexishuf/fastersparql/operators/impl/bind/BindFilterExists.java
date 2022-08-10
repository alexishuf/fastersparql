package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.FilterExists;
import com.github.alexishuf.fastersparql.operators.plan.ExistsPlan;
import com.github.alexishuf.fastersparql.operators.providers.FilterExistsProvider;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import static com.github.alexishuf.fastersparql.operators.impl.bind.NativeBindHelper.preferNative;

public final class BindFilterExists implements FilterExists {
    private final RowOperations rowOps;
    private final @Positive int bindConcurrency;

    public static class Provider implements FilterExistsProvider {
        @Override public @NonNegative int bid(long flags) {
            return BidCosts.BUILTIN_COST;
        }
        @Override public FilterExists create(long flags, RowOperations rowOperations) {
            return new BindFilterExists(rowOperations, BindJoin.Provider.concurrency(flags));
        }
    }

    public BindFilterExists(RowOperations rowOps, @Positive int bindConcurrency) {
        this.rowOps = rowOps;
        this.bindConcurrency = bindConcurrency;
    }

    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(ExistsPlan<R> plan) {
        return preferNative(rowOps, bindConcurrency, plan);
    }
}
