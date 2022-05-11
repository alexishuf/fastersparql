package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.FilterExists;
import com.github.alexishuf.fastersparql.operators.plan.FilterExistsPlan;
import com.github.alexishuf.fastersparql.operators.providers.FilterExistsProvider;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import static com.github.alexishuf.fastersparql.operators.impl.bind.NativeBindHelper.preferNative;

@Value  @Accessors(fluent = true)
public class BindFilterExists implements FilterExists {
    RowOperations rowOps;
    @Positive int bindConcurrency;

    public static class Provider implements FilterExistsProvider {

        @Override public @NonNegative int bid(long flags) {
            return BidCosts.BUILTIN_COST;
        }

        @Override public FilterExists create(long flags, RowOperations rowOperations) {
            return new BindFilterExists(rowOperations, BindJoin.Provider.concurrency(flags));
        }
    }

    @Override public <R> Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowOps.rowClass();
    }

    @Override public <R> Results<R> checkedRun(FilterExistsPlan<R> plan) {
        BindType type = plan.negate() ? BindType.NOT_EXISTS : BindType.EXISTS;
        return preferNative(rowOps, bindConcurrency, type, plan.input(), plan.filter());
    }
}
