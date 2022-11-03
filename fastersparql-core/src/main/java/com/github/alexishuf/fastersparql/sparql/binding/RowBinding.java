package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class RowBinding<R, I> extends Binding {
    private final RowType<R, I> rowType;
    private @Nullable R row;
    private @Nullable Object[] cache;

    public RowBinding(RowType<R, I> rowType, Vars vars) {
        super(vars);
        this.rowType = rowType;
    }

    public RowBinding<R, I> row(@Nullable R row) {
        this.row = row;
        return this;
    }

    @Override public Binding set(int i, @Nullable String value) {
        if (row == null)
            row = rowType.createEmpty(vars);
        rowType.setNT(row, i, value);
        return this;
    }

    @Override public @Nullable String get(int i) { return rowType.getNT(row, i); }

    @Override public @Nullable Term parse(String var) {
        int i = vars.indexOf(var);
        if (i == -1) return null;
        String nt = get(i);
        if (nt == null) return null;
        if (cache == null)
            cache = new Object[16];
        int keyBucket = nt.hashCode() & 7, valueBucket = keyBucket + 8;
        if (nt.equals(cache[keyBucket]))
            return (Term) cache[valueBucket];
        Term term = TermParser.parse(nt, 0, null);
        cache[keyBucket] = nt;
        cache[valueBucket] = term;
        return term;
    }
}