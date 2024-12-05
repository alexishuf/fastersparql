package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface PoolReceiver<T> {
    @Nullable T offer(T o, int len);
}
