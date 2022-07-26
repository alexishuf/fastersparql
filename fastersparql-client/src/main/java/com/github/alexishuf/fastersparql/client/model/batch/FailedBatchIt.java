package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBatchIt;
import com.github.alexishuf.fastersparql.client.util.async.RuntimeExecutionException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.NoSuchElementException;

public class FailedBatchIt<T> extends AbstractBatchIt<T> {
    private final Throwable error;
    private boolean ended;

    public FailedBatchIt(Class<T> elementClass, Throwable error) {
        this(elementClass, error, null);
    }

    public FailedBatchIt(Class<T> elementClass, Throwable error, @Nullable String name) {
        super(elementClass, name == null ? "Failed("+error.getClass().getSimpleName()+")" : name);
        this.error = error;
    }

    @Override public Batch<T> nextBatch() {
        throw new RuntimeExecutionException(error);
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        throw new RuntimeExecutionException(error);
    }

    @Override protected void cleanup() { }

    @Override public boolean hasNext() { return true; }

    @Override public T next() {
        if (ended)
            throw new NoSuchElementException();
        ended = true;
        throw new RuntimeExecutionException(error);
    }
}
