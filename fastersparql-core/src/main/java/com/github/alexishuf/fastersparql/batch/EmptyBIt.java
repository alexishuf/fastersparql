package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Collection;
import java.util.NoSuchElementException;

public final class EmptyBIt<T> extends AbstractBIt<T> {
    public EmptyBIt(Class<T> elementClass, Vars vars) { super(elementClass, vars); }
    public EmptyBIt(Class<T> elementClass)            { super(elementClass, Vars.EMPTY); }

    @Override public boolean  recycle(Batch<T> batch)                      { return false; }
    @Override public int      nextBatch(Collection<? super T> destination) { return 0; }
    @Override public Batch<T> nextBatch() { return Batch.terminal(); }

    @Override public    @This BIt<T> tempEager()                  { return this; }
    @Override protected void         cleanup(boolean interrupted) { }
    @Override public    boolean      hasNext()                    { return false; }
    @Override public    T            next()                       { throw new NoSuchElementException(); }
    @Override public    String       toString()                   { return "EMPTY"; }
}
