package com.github.alexishuf.fastersparql.util;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Arrays.asList;

public class AutoCloseableSet<T extends AutoCloseable> extends ArrayList<T> implements AutoCloseable {
    public AutoCloseableSet() { }
    public AutoCloseableSet(Collection<? extends T> c) { super(c); }

    @SafeVarargs
    public static <U extends AutoCloseable> AutoCloseableSet<U> of(U... instances) {
        return new AutoCloseableSet<>(asList(instances));
    }

    public <U extends T> U put(U object) {
        add(object);
        return object;
    }

    @Override public void close() {
        ExceptionCondenser.closeAll(RuntimeException.class, RuntimeException::new, this);
    }
}
