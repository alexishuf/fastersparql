package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;

public class SingletonBIt<T> extends AbstractBIt<T> {
    private final @Nullable T value;
    private boolean ended;

    public SingletonBIt(T value, Vars vars) {
        //noinspection unchecked
        this(value, (Class<T>) value.getClass(), vars);
    }

    public SingletonBIt(@Nullable T value, Class<T> elementClass, Vars vars) {
        super(elementClass, vars);
        this.value = value;
    }

    @Override public @This BIt<T> tempEager() { return this; }

    @Override public Batch<T> nextBatch() {
        Batch<T> batch = ended ? Batch.terminal() : new Batch<>(elementClass, 1);
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

    @Override public    boolean recycle(Batch<T> batch)      { return false; }
    @Override protected void    cleanup(boolean interrupted) { }
    @Override public    boolean hasNext()                    { return !ended; }
    @Override public    String  toString()                   { return Objects.toString(value); }

    @Override public T next() {
        if (ended)
            throw new NoSuchElementException();
        ended = true;
        onExhausted();
        return value;
    }
}
