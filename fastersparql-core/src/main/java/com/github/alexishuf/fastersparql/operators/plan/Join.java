package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.bit.NativeBind;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public final class Join extends Plan {
    public @Nullable Vars projection;

    public Join(Plan left, Plan right) { this(null, left, right); }

    public Join(@Nullable Vars projection, Plan left, Plan right) {
        super(Operator.JOIN);
        this.projection = projection;
        this.left = left;
        this.right = right;
    }

    public Join(Plan... operands) { this(null, operands); }
    public Join(@Nullable Vars projection,  Plan... operands) {
        super(Operator.JOIN);
        this.projection = projection;
        replace(operands);
    }

    @Override public Join copy(@Nullable Plan[] ops) {
        if (ops == null) ops = operandsArray;
        return ops == null ? new Join(projection, left, right) : new Join(projection, ops);
    }

    @Override
    public <R> BIt<R> execute(RowType<R> rt, @Nullable Binding binding, boolean canDedup) {
        return NativeBind.preferNative(rt, this, binding, canDedup);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Join r && Objects.equals(projection, r.projection) && super.equals(r);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), projection);
    }
}
