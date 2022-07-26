package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBatchIt;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.NoSuchElementException;

public class SingletonBatchIterator<T> extends AbstractBatchIt<T> {
    private final @Nullable T value;
    private boolean ended;

    public SingletonBatchIterator(T value) { this(value, (String) null); }

    public SingletonBatchIterator(T value, @Nullable String name) {
        //noinspection unchecked
        this(value, (Class<T>) value.getClass(), name);
    }

    public SingletonBatchIterator(@Nullable T value, Class<T> elementClass) {
        this(value, elementClass, null);
    }

    public SingletonBatchIterator(@Nullable T value, Class<T> elementClass, @Nullable String name) {
        super(elementClass, name == null ? "Singleton("+value+")" : name);
        this.value = value;
    }

    @Override public Batch<T> nextBatch() {
        int size = ended ? 0 : 1;
        Batch<T> batch = new Batch<>(elementClass, size, size);
        if (!ended) {
            batch.array[0] = value;
            ended = true;
        }
        return batch;
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        if (ended) {
            return 0;
        } else {
            destination.add(value);
            ended = true;
            return 1;
        }
    }

    @Override protected void cleanup() { }

    @Override public boolean hasNext() {
        return !ended;
    }

    @Override public T next() {
        if (ended)
            throw new NoSuchElementException();
        ended = true;
        return value;
    }
}
