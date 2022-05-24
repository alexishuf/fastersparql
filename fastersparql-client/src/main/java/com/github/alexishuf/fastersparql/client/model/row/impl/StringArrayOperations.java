package com.github.alexishuf.fastersparql.client.model.row.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;

import java.util.List;

public class StringArrayOperations extends ArrayOperations {
    private static final StringArrayOperations INSTANCE = new StringArrayOperations(String[].class);

    public static class Provider extends ArrayOperations.Provider {
        @Override public RowOperations get(Class<?> specializedClass) {
            if (!String[].class.isAssignableFrom(specializedClass))
                throw new IllegalArgumentException("Expected String[], got "+specializedClass);
            return INSTANCE;
        }

        @Override public Class<?> rowClass() { return String[].class; }
    }

    public static StringArrayOperations get() {
        return INSTANCE;
    }

    public StringArrayOperations(Class<?> rowClass) {
        super(rowClass);
        assert String[].class.isAssignableFrom(rowClass);
    }

    @Override public Object createEmpty(List<String> vars) {
        return new String[vars.size()];
    }
}
