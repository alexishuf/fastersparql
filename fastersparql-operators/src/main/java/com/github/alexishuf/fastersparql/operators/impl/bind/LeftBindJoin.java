package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.LeftJoin;
import com.github.alexishuf.fastersparql.operators.plan.LeftJoinPlan;
import com.github.alexishuf.fastersparql.operators.providers.LeftJoinProvider;
import org.checkerframework.checker.index.qual.NonNegative;

import static com.github.alexishuf.fastersparql.operators.impl.bind.NativeBindHelper.preferNative;

public final class LeftBindJoin implements LeftJoin {
    private final RowOperations rowOps;
    private final int bindConcurrency;

    public static class Provider implements LeftJoinProvider {
        @Override public @NonNegative int bid(long flags) {
            return BindJoin.Provider.bindCost(flags);
        }
        @Override public LeftJoin create(long flags, RowOperations rowOperations) {
            return new LeftBindJoin(rowOperations, BindJoin.Provider.concurrency(flags));
        }
    }

    public LeftBindJoin(RowOperations rowOps, int bindConcurrency) {
        this.rowOps = rowOps;
        this.bindConcurrency = bindConcurrency;
    }

    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(LeftJoinPlan<R> plan) {
        return preferNative(rowOps, bindConcurrency, plan);
    }
}
