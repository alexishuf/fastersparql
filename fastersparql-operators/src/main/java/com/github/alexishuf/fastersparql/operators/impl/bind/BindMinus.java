package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.Minus;
import com.github.alexishuf.fastersparql.operators.plan.MinusPlan;
import com.github.alexishuf.fastersparql.operators.providers.MinusProvider;
import org.checkerframework.checker.index.qual.NonNegative;

import static com.github.alexishuf.fastersparql.operators.impl.bind.NativeBindHelper.preferNative;

public final class BindMinus implements Minus {
    private final RowOperations rowOps;
    private final int bindConcurrency;

    public static class Provider implements MinusProvider {
        @Override public @NonNegative int bid(long flags) {
            return BindJoin.Provider.bindCost(flags);
        }
        @Override public Minus create(long flags, RowOperations rowOperations) {
            return new BindMinus(rowOperations, BindJoin.Provider.concurrency(flags));
        }
    }

    public BindMinus(RowOperations rowOps, int bindConcurrency) {
        this.rowOps = rowOps;
        this.bindConcurrency = bindConcurrency;
    }

    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(MinusPlan<R> plan) {
        return preferNative(rowOps, bindConcurrency, plan);
    }
}
