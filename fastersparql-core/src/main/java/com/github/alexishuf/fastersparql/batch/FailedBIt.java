package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Collection;
import java.util.NoSuchElementException;

public class FailedBIt<T> extends AbstractBIt<T> {
    private final RuntimeException error;
    private boolean ended;

    public FailedBIt(RowType<T> rowType, Vars vars, Throwable error) {
        super(rowType, vars);
        this.error = error instanceof RuntimeException re ? re : new RuntimeException(error);
    }

    public FailedBIt(RowType<T> rowType, Throwable error) {
        this(rowType, Vars.EMPTY, error);
    }

    @Override public    @This BIt<T> tempEager()                        { return this; }
    @Override public    int          nextBatch(Collection<? super T> d) { throw error; }
    @Override public    Batch<T>     nextBatch()                        { throw error; }
    @Override public    boolean      hasNext()                          { return true; }
    @Override public    String       toString()                         { return error.toString(); }

    @Override public T next() {
        if (ended)
            throw new NoSuchElementException();
        ended = true;
        onTermination(error);
        throw error;
    }
}
