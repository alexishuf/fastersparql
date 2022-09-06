package com.github.alexishuf.fastersparql.client.model.batch;

import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBIt;

import java.util.Collection;
import java.util.NoSuchElementException;

public final class EmptyBIt<T> extends AbstractBIt<T> {
    public EmptyBIt(Class<T> elementClass) { this(elementClass, "EMPTY"); }
    public EmptyBIt(Class<T> elementClass, String name) { super(elementClass, name); }

    @Override public boolean  recycle(Batch<T> batch)                      { return false; }
    @Override public int      nextBatch(Collection<? super T> destination) { return 0; }
    @Override public Batch<T> nextBatch() { return new Batch<>(elementClass, 0); }

    @Override protected void     cleanup()   { }
    @Override public    boolean  hasNext()   { return false; }
    @Override public    T        next()      { throw new NoSuchElementException(); }
}
