package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.Minus;
import com.github.alexishuf.fastersparql.operators.plan.MinusPlan;
import com.github.alexishuf.fastersparql.operators.providers.MinusProvider;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.NonNegative;

import static com.github.alexishuf.fastersparql.operators.impl.bind.NativeBindHelper.preferNative;

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

    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(MinusPlan<R> plan) {
        return preferNative(rowOps, bindConcurrency, plan);
    }
}
