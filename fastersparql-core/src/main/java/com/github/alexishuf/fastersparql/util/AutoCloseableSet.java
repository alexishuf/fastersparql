package com.github.alexishuf.fastersparql.util;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Arrays.asList;

public class AutoCloseableSet<T extends AutoCloseable> extends ArrayList<T> implements AutoCloseable {
    private static final AutoCloseableSet<?> EMPTY = new AutoCloseableSet<>();

    public AutoCloseableSet() { }
    public AutoCloseableSet(Collection<? extends T> c) { super(c); }

    public static <T extends AutoCloseable> AutoCloseableSet<T> empty() { //noinspection unchecked
        return (AutoCloseableSet<T>) EMPTY;
    }

    @SafeVarargs
    public static <U extends AutoCloseable> AutoCloseableSet<U> of(U... instances) {
        return new AutoCloseableSet<>(asList(instances));
    }

    public <U extends T> U put(U object) {
        add(object);
        return object;
    }

    @Override public void close() {
        ExceptionCondenser.closeAll(this);
    }
}
