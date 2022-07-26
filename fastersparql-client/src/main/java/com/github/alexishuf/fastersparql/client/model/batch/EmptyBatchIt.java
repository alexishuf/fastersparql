package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBatchIt;

import java.util.Collection;
import java.util.NoSuchElementException;

public class EmptyBatchIt<T> extends AbstractBatchIt<T> {
    public EmptyBatchIt(Class<T> elementClass) {
        this(elementClass, "EMPTY");
    }

    public EmptyBatchIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }

    @Override public    int      nextBatch(Collection<? super T> destination) { return 0; }
    @Override public    Batch<T> nextBatch() { return new Batch<>(elementClass); }
    @Override protected void     cleanup()   { }
    @Override public    boolean  hasNext()   { return false; }
    @Override public    T        next()      { throw new NoSuchElementException(); }
}
