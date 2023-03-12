package com.github.alexishuf.fastersparql.util;

public interface NamedService<N> {
    N name();
    default int order() { return 0; }
}
