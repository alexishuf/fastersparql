package com.github.alexishuf.fastersparql.operators.row.impl;

import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsProvider;

import java.util.List;

public class StringArrayOperations extends ArrayOperations {
    private static final StringArrayOperations INSTANCE = new StringArrayOperations();
    public static class Provider implements RowOperationsProvider {
        @Override public RowOperations get() { return INSTANCE; }
        @Override public Class<?> rowClass() { return String[].class; }
    }

    @Override public Object createEmpty(List<String> vars) {
        return new String[vars.size()];
    }
}
