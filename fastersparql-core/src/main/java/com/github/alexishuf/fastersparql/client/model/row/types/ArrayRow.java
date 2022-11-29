package com.github.alexishuf.fastersparql.client.model.row.types;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;

@SuppressWarnings("unchecked")
public class ArrayRow<I> extends RowType<I[], I> {
    public static final ArrayRow<String> STRING = new ArrayRow<>(String[].class, String.class);

    public ArrayRow(Class<I[]> arrayClass, Class<I> itemClass) { super(arrayClass, itemClass); }
    public ArrayRow(Class<I> itemClass) {
        super((Class<I[]>)Array.newInstance(itemClass, 0).getClass(), itemClass);
    }

    @Override public @Nullable I set(@Nullable I[] row, int idx, @Nullable I item) {
        if (row == null)
            return null;
        I old = row[idx];
        row[idx] = item;
        return old;
    }

    @Override public @Nullable I get(@Nullable I[] row, int idx) {
        return row != null ? row[idx] : null;
    }

    @Override public I[] createEmpty(Vars vars) {
        return (I[]) Array.newInstance(itemClass, vars.size());
    }

    @Override public boolean equalsSameVars(I[] left, I[] right) {
        return Arrays.equals(left, right);
    }

    @Override public int hash(@Nullable Object row) { return Arrays.hashCode((I[]) row); }

    @Override public String toString(@Nullable Object row) {
         return row == null ? "[]" : Arrays.toString( (I[]) row);
    }
}
