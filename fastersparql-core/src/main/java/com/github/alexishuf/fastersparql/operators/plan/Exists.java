package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.operators.bit.NativeBind;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public final class Exists extends Plan {
    final boolean negate;

    public Exists(Plan in, boolean negate, Plan filter) {
        super(negate ? Operator.NOT_EXISTS : Operator.EXISTS);
        this.negate = negate;
        this.left = in;
        this.right = filter;
    }

    public Plan  input()    { return left; }
    public Plan filter()    { return right; }
    @SuppressWarnings("unused")
    public boolean negate() { return negate; }

    @Override public Plan copy(@Nullable Plan[] ops) {
        return ops == null ? new Exists(left,  negate, right)
                           : new Exists(ops[0], negate, ops[1]);
    }

    @Override
    public <B extends Batch<B>>
    BIt<B> execute(BatchType<B> bt, @Nullable Binding binding, boolean canDedup) {
        return NativeBind.preferNative(bt, this, binding, canDedup);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Exists e && negate == e.negate
                && Objects.equals (left, e.left )
                && Objects.equals(right, e.right);
    }

    @Override public int hashCode() {
        return Objects.hash(negate, left, right);
    }
}
