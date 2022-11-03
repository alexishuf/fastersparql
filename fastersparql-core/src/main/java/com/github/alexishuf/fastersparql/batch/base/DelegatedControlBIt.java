package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public abstract class DelegatedControlBIt<T, S> implements BIt<T> {
    protected final BIt<S> delegate;
    protected final Class<T> elementClass;
    protected final Vars vars;

    public DelegatedControlBIt(BIt<S> delegate, Class<T> elementClass, Vars vars) {
        this.delegate = delegate;
        this.elementClass = elementClass;
        this.vars = vars;
    }

    public BIt<S> delegate() { return delegate; }

    @Override public final Class<T>     elementClass() { return elementClass; }
    @Override public final Vars vars()         { return vars; }

    protected String toStringNoArgs() {
        String name = getClass().getSimpleName();
        int suffixStart = name.length() - 3;
        if (name.regionMatches(suffixStart, "BIt", 0, 3))
            return name.substring(0, suffixStart);
        return name;
    }

    @Override public String toString() {
        return toStringNoArgs()+'('+delegate.toString()+')';
    }

    @Override @This public BIt<T> minWait(long time, TimeUnit unit) {
        delegate.minWait(time, unit);
        return this;
    }

    @Override @This public BIt<T> minWait(Duration duration) {
        delegate.minWait(duration);
        return this;
    }

    @Override @This public BIt<T> maxWait(long time, TimeUnit unit) {
        delegate.maxWait(time, unit);
        return this;
    }

    @Override @This public BIt<T> maxWait(Duration duration) {
        delegate.maxWait(duration);
        return this;
    }

    @Override public BIt<T> minBatch(int size) {
        delegate.minBatch(size);
        return this;
    }

    @Override @This public BIt<T> maxBatch(int size) {
        delegate.maxBatch(size);
        return this;
    }

    @Override public long         minWait(TimeUnit unit)  { return delegate.minWait(unit); }
    @Override public long         maxWait(TimeUnit unit)  { return delegate.maxWait(unit); }
    @Override public int          minBatch()              { return delegate.minBatch(); }
    @Override public @This BIt<T> tempEager()             { delegate.tempEager(); return this; }
    @Override public int          maxBatch()              { return delegate.maxBatch(); }
    @Override public void         close()                 { delegate.close(); }
    @Override public boolean      hasNext()               { return delegate.hasNext(); }
}
