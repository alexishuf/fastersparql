package com.github.alexishuf.fastersparql.client.model.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class NullRowOperations implements RowOperations {
    private static final Map<Class<?>, NullRowOperations> CACHE = new ConcurrentHashMap<>();
    private final Class<?> rowClass;
    public NullRowOperations(Class<?> rowClass) { this.rowClass = rowClass; }

    public static NullRowOperations of(Class<?> rowClass) {
        return CACHE.computeIfAbsent(rowClass, NullRowOperations::new);
    }

    public Class<?> rowClass() { return rowClass; }

    @Override public Object set(Object row, int idx, String var, Object object) {
        throw new UnsupportedOperationException(
                format("%s cannot set(%s, %d, %s, %s)", this, row, idx, var, object));
    }

    @Override public Object get(Object row, int idx, String var) {
        return null;
    }

    @Override public @Nullable String getNT(@Nullable Object row, int idx, String var) {
        return null;
    }

    @Override public Object createEmpty(List<String> vars) {
        throw new UnsupportedOperationException(
                format("%s cannot createEmpty(%s)", this, vars));
    }

    @Override public boolean equalsSameVars(Object left, Object right) {
        return Objects.equals(left, right);
    }

    @Override public int hash(@Nullable Object row) {
        return Objects.hashCode(row);
    }

    @Override public boolean needsCustomHash() {
        return false;
    }

    @Override public String toString(@Nullable Object row) {
        return row == null ? "[]" : row.toString();
    }
}
