package com.github.alexishuf.fastersparql.client.model.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsProvider;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@RequiredArgsConstructor
@Getter @Accessors(fluent = true)
public class ArrayOperations implements RowOperations {
    private static final ConcurrentHashMap<Class<?>, ArrayOperations> CACHE
            = new ConcurrentHashMap<>();
    public static final Provider PROVIDER = new Provider();
    private final Class<?> rowClass;

    public static class Provider implements RowOperationsProvider {


        @Override public RowOperations get(Class<?> specializedClass) {
            if (!Object[].class.isAssignableFrom(specializedClass))
                throw new IllegalArgumentException(specializedClass+" is not an object array class");
            Function<Class<?>, ArrayOperations> fac;
            if (String[].class.isAssignableFrom(specializedClass))
                return StringArrayOperations.get();
            else if (CharSequence[].class.isAssignableFrom(specializedClass))
                fac = CharSequenceArrayOperations::new;
            else
                fac = ArrayOperations::new;
            return CACHE.computeIfAbsent(specializedClass, fac);
        }
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
        Class<?> componentType = rowClass.getComponentType();
        assert componentType != null;
        return Array.newInstance(componentType, vars.size());
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

    @Override public String toString(@Nullable Object row) {
        if (row == null) return "[]";
        return Arrays.toString((Object[]) row);
    }
}
