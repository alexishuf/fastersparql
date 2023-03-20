package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.operators.bit.NativeBind;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public final class Minus extends Plan {
    public Minus(Plan left, Plan right) {
        super(Operator.MINUS);
        this.left  = left;
        this.right = right;
    }

    @Override public Plan copy(@Nullable Plan[] ops) {
        return ops == null ? new Minus(left, right) : new Minus(ops[0], ops[1]);
    }

    @Override
    public <B extends Batch<B>>
    BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean canDedup) {
        return NativeBind.preferNative(bt, this, binding, canDedup);
    }

    @Override public boolean equals(Object obj) {
        return obj instanceof Minus r
                && Objects.equals(left , r.left )
                && Objects.equals(right, r.right);
    }

    @Override public int hashCode() {
        return Objects.hash(type, left, right);
    }
}
