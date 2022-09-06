package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBIt;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;

public class SingletonBIt<T> extends AbstractBIt<T> {
    private final @Nullable T value;
    private boolean ended;

    public SingletonBIt(T value) { this(value, (String) null); }

    public SingletonBIt(T value, @Nullable String name) {
        //noinspection unchecked
        this(value, (Class<T>) value.getClass(), name);
    }

    public SingletonBIt(@Nullable T value, Class<T> elementClass) {
        this(value, elementClass, null);
    }

    public SingletonBIt(@Nullable T value, Class<T> elementClass, @Nullable String name) {
        super(elementClass, name == null ? Objects.toString(value) : name);
        this.value = value;
    }

    @Override public Batch<T> nextBatch() {
        var batch = new Batch<>(elementClass, ended ? 0 : 1);
        if (!ended) {
            batch.add(value);
            ended = true;
            onExhausted();
        }
        return batch;
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        if (ended) {
            return 0;
        } else {
            destination.add(value);
            ended = true;
            onExhausted();
            return 1;
        }
    }

    @Override public    boolean recycle(Batch<T> batch) { return false; }
    @Override protected void    cleanup()               { }
    @Override public    boolean hasNext()               { return !ended; }

    @Override public T next() {
        if (ended)
            throw new NoSuchElementException();
        ended = true;
        onExhausted();
        return value;
    }
}
