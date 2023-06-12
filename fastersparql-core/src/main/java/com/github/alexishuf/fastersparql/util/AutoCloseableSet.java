package com.github.alexishuf.fastersparql.util;

import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Arrays.asList;

public class AutoCloseableSet<T extends AutoCloseable> extends ArrayList<T>
        implements AutoCloseable {
    private static final AutoCloseableSet<?> EMPTY = new AutoCloseableSet<>();
    private boolean parallelClose = false;

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

    public @This AutoCloseableSet<T> parallelClose(boolean value) {
        this.parallelClose = value;
        return this;
    }

    @Override public void close() {
        if (parallelClose)
            ExceptionCondenser.parallelCloseAll(this);
        else
            ExceptionCondenser.closeAll(this);
    }
}
