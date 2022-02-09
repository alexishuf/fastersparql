package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.operators.row.RowMatcher;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;

public class ArrayOperations implements RowOperations {
    public static final ArrayOperations INSTANCE = new ArrayOperations();

    public static class Provider implements RowOperationsProvider {
        @Override public RowOperations get() { return INSTANCE; }
        @Override public Class<?> rowClass() { return Object[].class; }
    }

    @Override public @Nullable Object set(@Nullable Object row, int idx, String var, @Nullable Object object) {
        if (row == null)
            return null;
        Object[] array = (Object[]) row;
        Object old = array[idx];
        array[idx] = object;
        return old;
    }

    @Override public @Nullable Object get(@Nullable Object row, int idx, String var) {
        return row != null ? ((Object[]) row)[idx] : null;
    }

    @Override public @Nullable String getNT(@Nullable Object row, int idx, String var) {
        Object value = get(row, idx, var);
        return value == null ? null : value.toString();
    }

    @Override public Object createEmpty(List<String> vars) {
        return new Object[vars.size()];
    }

    @Override public boolean equalsSameVars(Object left, Object right) {
        return Arrays.equals((Object[]) left, (Object[])right);
    }

    @Override public int hash(@Nullable Object row) {
        return Arrays.hashCode((Object[]) row);
    }

    @Override public boolean needsCustomHash() {
        return true;
    }

    @Override public RowMatcher createMatcher(List<String> leftVars, List<String> rightVars) {
        return new ArrayMatcher(leftVars, rightVars);
    }
}
