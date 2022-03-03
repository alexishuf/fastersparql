package com.github.alexishuf.fastersparql.operators.row.impl;

import java.util.List;

public class StringArrayOperations extends ArrayOperations {
    public static class Provider extends ArrayOperations.Provider {
        @Override public Class<?> rowClass() { return String[].class; }
    }

    public static StringArrayOperations get() {
        return (StringArrayOperations) ArrayOperations.PROVIDER.get(String[].class);
    }

    public StringArrayOperations(Class<?> rowClass) {
        super(rowClass);
        assert String[].class.isAssignableFrom(rowClass);
    }

    @Override public Object createEmpty(List<String> vars) {
        return new String[vars.size()];
    }
}
