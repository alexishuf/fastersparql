package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBIt;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.NoSuchElementException;

public class FailedBIt<T> extends AbstractBIt<T> {
    private final RuntimeException error;
    private boolean ended;

    public FailedBIt(Class<T> elementClass, Throwable error) {
        this(elementClass, error, null);
    }

    public FailedBIt(Class<T> elementClass, Throwable error, @Nullable String name) {
        super(elementClass, name == null ? "Failed("+error.getClass().getSimpleName()+")" : name);
        this.error = error instanceof RuntimeException re ? re : new RuntimeException(error);
    }

    @Override public    int      nextBatch(Collection<? super T> d) { throw error; }
    @Override public    Batch<T> nextBatch()                        { throw error; }
    @Override public    boolean  recycle(Batch<T> batch)            { return false; }
    @Override protected void     cleanup()                          { }
    @Override public    boolean  hasNext()                          { return true; }

    @Override public T next() {
        if (ended)
            throw new NoSuchElementException();
        ended = true;
        throw error;
    }
}
