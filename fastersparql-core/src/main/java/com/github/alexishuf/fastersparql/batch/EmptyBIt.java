package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Collection;
import java.util.NoSuchElementException;

public final class EmptyBIt<T> extends AbstractBIt<T> {
    public EmptyBIt(RowType<T> rowType, Vars vars) { super(rowType, vars); }
    public EmptyBIt(RowType<T> rowType) { super(rowType, Vars.EMPTY); }

    @Override public int      nextBatch(Collection<? super T> destination) { return 0; }
    @Override public Batch<T> nextBatch() { return Batch.terminal(); }

    @Override public    @This BIt<T> tempEager()                  { return this; }
    @Override public    boolean      hasNext()                    { return false; }
    @Override public    T            next()                       { throw new NoSuchElementException(); }
    @Override public    String       toString()                   { return "EMPTY"; }
}
