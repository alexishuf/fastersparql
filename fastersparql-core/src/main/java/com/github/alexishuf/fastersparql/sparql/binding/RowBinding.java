package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class RowBinding<R> extends Binding {
    private final RowType<R> rowType;
    private @Nullable R row;

    public RowBinding(RowType<R> rowType, Vars vars) {
        super(vars);
        this.rowType = rowType;
    }

    public RowBinding<R> row(@Nullable R row) {
        this.row = row;
        return this;
    }

    @Override public Binding set(int column, @Nullable Term value) {
        int n = vars.size();
        if (row == null) {
            row = rowType.builder(n).set(column, value).build();
        } else {
            var b = rowType.builder(n);
            for (int i = 0; i < n; i++)
                b.set(i, rowType.get(row, i));
            b.set(column, value);
            row = b.build();
        }
        return this;
    }

    @Override public @Nullable Term get(int i) { return rowType.get(row, i); }
}