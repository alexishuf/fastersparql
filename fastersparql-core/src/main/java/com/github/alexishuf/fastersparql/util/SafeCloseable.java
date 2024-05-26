package com.github.alexishuf.fastersparql.util;

public interface SafeCloseable extends AutoCloseable {
    @Override void close();
}
