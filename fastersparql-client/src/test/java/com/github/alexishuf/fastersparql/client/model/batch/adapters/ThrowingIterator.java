package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

public final class ThrowingIterator<T> implements Iterator<T> {
    private final Iterator<T> delegate;
    private final Throwable error;
    private boolean threw = false;

    private ThrowingIterator(Iterator<T> delegate, Throwable error) {
        this.delegate = delegate;
        this.error = error;
    }

    public static <U> Iterator<U> andThrow(Iterator<U> it, @Nullable Throwable error) {
        return error == null ? it : new ThrowingIterator<>(it, error);
    }

    @Override public boolean hasNext() {
        if (delegate.hasNext())
            return true;
        else if (threw)
            return false;
        threw = true;
        throw error instanceof RuntimeException e ? e : new RuntimeException(error);
    }

    @Override public T next() { return delegate.next(); }

    @Override public String toString() {
        return "ThrowingIterator["+error.getClass().getSimpleName()+"]("+delegate+")";
    }
}
