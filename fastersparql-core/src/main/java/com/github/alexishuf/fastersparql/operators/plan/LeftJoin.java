package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.bit.NativeBind;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public final class LeftJoin extends Plan {
    public LeftJoin(Plan left, Plan right) {
        super(Operator.LEFT_JOIN);
        this.left  = left;
        this.right = right;
    }

    @Override public Plan copy(@Nullable Plan[] ops) {
        return ops == null ? new LeftJoin(left, right) : new LeftJoin(ops[0], ops[1]);
    }

    @Override
    public <B extends Batch<B>>
    BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean weakDedup) {
        if (right instanceof Query q && q.sparql.isAsk())
            return left().execute(bt, binding, weakDedup);
        return NativeBind.preferNative(bt, this, binding, weakDedup);
    }

    @Override
    public <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> type, Vars rebindHint, boolean weakDedup) {
        if (right instanceof Query q && q.sparql.isAsk())
            return left().emit(type, rebindHint, weakDedup);
        return NativeBind.preferNativeEmit(type, this, rebindHint, weakDedup);
    }

    @Override public boolean equals(Object obj) {
        return obj instanceof LeftJoin r
                && Objects.equals(left , r.left )
                && Objects.equals(right, r.right);
    }

    @Override public int hashCode() {
        return Objects.hash(type, left, right);
    }
}
