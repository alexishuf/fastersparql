package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.operators.row.RowMatcher;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;

public class NullRowOperations implements RowOperations {
    public static final NullRowOperations INSTANCE = new NullRowOperations();

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

    @Override public RowMatcher createMatcher(List<String> leftVars, List<String> rightVars) {
        throw new UnsupportedOperationException(
                format("%s cannot createMatcher(%s, %s)", this, leftVars, rightVars));
    }
}
