package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.operators.row.RowMatcher;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsProvider;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Value @Accessors(fluent = true)
public class ListOperations implements RowOperations {
    private static final ConcurrentHashMap<Class<?>, ListOperations> CACHE
            = new ConcurrentHashMap<>();
    public static final Provider PROVIDER = new Provider();
    Class<?> rowClass;

    public static class Provider implements RowOperationsProvider {
        @Override public RowOperations get(Class<?> specializedClass) {
            if (!List.class.isAssignableFrom(specializedClass))
                throw new IllegalArgumentException(specializedClass+" is not a List");
            return CACHE.computeIfAbsent(specializedClass, ListOperations::new);
        }
        @Override public Class<?> rowClass() { return List.class; }
    }

    public static ListOperations get() {
        return (ListOperations) PROVIDER.get(List.class);
    }

    @Override public Object set(Object row, int idx, String var, Object object) {
        if (row == null)
            return null;
        //noinspection unchecked
        List<Object> list = (List<Object>) row;
        Object old = list.get(idx);
        list.set(idx, object);
        return old;
    }

    @Override public Object get(Object row, int idx, String var) {
        return row == null ? null : ((List<?>)row).get(idx);
    }

    @Override public @Nullable String getNT(@Nullable Object row, int idx, String var) {
        Object value = get(row, idx, var);
        return value == null ? null : value.toString();
    }

    @Override public Object createEmpty(List<String> vars) {
        int size = vars.size();
        ArrayList<?> row = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            row.add(null);
        return row;
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

    @Override public RowMatcher createMatcher(List<String> leftVars, List<String> rightVars) {
        return new ListMatcher(leftVars, rightVars);
    }
}
