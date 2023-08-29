package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.base.AbstractBIt.cls2name;
import static com.github.alexishuf.fastersparql.exceptions.FSCancelledException.isCancel;

public abstract class DelegatedControlBIt<B extends Batch<B>, S extends Batch<S>> implements BIt<B> {
    private static final Logger log = LoggerFactory.getLogger(DelegatedControlBIt.class);
    public static final VarHandle TERM;
    static {
        try {
            TERM = MethodHandles.lookup().findVarHandle(DelegatedControlBIt.class, "plainTerminated", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected BIt<S> delegate;
    protected final BatchType<B> batchType;
    protected final Vars vars;
    protected @Nullable MetricsFeeder metrics;
    @SuppressWarnings("unused") private boolean plainTerminated;

    public DelegatedControlBIt(BIt<S> delegate, BatchType<B> batchType, Vars vars) {
        this.delegate = delegate;
        this.batchType = batchType;
        this.vars = vars;
    }

    public BIt<S> delegate() { return delegate; }

    @Override public Stream<? extends StreamNode> upstream() {
        return Optional.ofNullable(delegate).stream();
    }

    protected abstract void cleanup(boolean cancelled, @Nullable Throwable error);

    protected void onTermination(boolean cancelled, @Nullable Throwable error) {
        if ((boolean)TERM.compareAndExchangeRelease(this, false, true)) {
            log.trace("Ignoring onTermination({}, {}) on terminated {}", cancelled, error, this);
        } else {
            try {
                cleanup(cancelled, error);
            } catch (Throwable t) {
                log.warn("Ignoring cleanup() failure for {}", this, t);
            }
        }
    }

    @Override public void close() {
        if (!(boolean)TERM.getAcquire(this))
            onTermination(true, null);
        delegate.close();
    }

    @Override public BatchType<B> batchType() { return batchType; }

    @Override public final Vars vars()    { return vars; }

    @Override public @This BIt<B> metrics(@Nullable MetricsFeeder metrics) {
        this.metrics = metrics;
        if (metrics != null && delegate instanceof AbstractBIt<S> i && i.state().isTerminated())
            metrics.completeAndDeliver(i.error, isCancel(i.error));
        return this;
    }

    @Override public String toString() {
        return cls2name(getClass()) +'('+delegate.toString()+')';
    }

    @Override public BIt<B> preferred() {
        delegate.preferred();
        return this;
    }

    @Override public BIt<B> quickWait() {
        delegate.quickWait();
        return this;
    }

    @Override public @This BIt<B> eager() {
        delegate.eager();
        return this;
    }

    @Override public @This BIt<B> tempEager() {
        delegate.tempEager(); return this;
    }

    @Override @This public BIt<B> minWait(long time, TimeUnit unit) {
        delegate.minWait(time, unit);
        return this;
    }

    @Override @This public BIt<B> minWait(Duration duration) {
        delegate.minWait(duration);
        return this;
    }

    @Override @This public BIt<B> maxWait(long time, TimeUnit unit) {
        delegate.maxWait(time, unit);
        return this;
    }

    @Override @This public BIt<B> maxWait(Duration duration) {
        delegate.maxWait(duration);
        return this;
    }

    @Override public BIt<B> minBatch(int rows) {
        delegate.minBatch(rows);
        return this;
    }

    @Override @This public BIt<B> maxBatch(int size) {
        delegate.maxBatch(size);
        return this;
    }
    @Override public long         minWait(TimeUnit unit)  { return delegate.minWait(unit); }
    @Override public long         maxWait(TimeUnit unit)  { return delegate.maxWait(unit); }
    @Override public int          minBatch()              { return delegate.minBatch(); }
    @Override public int          maxBatch()              { return delegate.maxBatch(); }
}
