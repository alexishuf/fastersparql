package com.github.alexishuf.fastersparql.util;

public interface NamedService<N> {
    N name();
    @SuppressWarnings("SameReturnValue") default int order() { return 0; }
}
