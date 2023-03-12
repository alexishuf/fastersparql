package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;

public class SingletonBIt<T> extends AbstractBIt<T> {
    private final @Nullable T value;
    private boolean ended;

    public SingletonBIt(@Nullable T value, RowType<T> rowType, Vars vars) {
        super(rowType, vars);
        this.value = value;
    }

    @Override public @This BIt<T> tempEager() { return this; }

    @Override public Batch<T> nextBatch() {
        Batch<T> batch = ended ? Batch.terminal() : new Batch<>(rowType.rowClass, 1);
        if (!ended) {
            batch.add(value);
            ended = true;
            onTermination(null);
        }
        return batch;
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        if (ended) {
            return 0;
        } else {
            destination.add(value);
            ended = true;
            onTermination(null);
            return 1;
        }
    }

    @Override public    boolean hasNext()                    { return !ended; }
    @Override public    String  toString()                   { return Objects.toString(value); }

    @Override public T next() {
        if (ended)
            throw new NoSuchElementException();
        ended = true;
        onTermination(null);
        return value;
    }
}
