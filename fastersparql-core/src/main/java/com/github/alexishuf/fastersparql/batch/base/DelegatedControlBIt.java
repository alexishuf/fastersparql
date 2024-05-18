package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItCancelledException;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
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

import static com.github.alexishuf.fastersparql.batch.BIt.State.*;
import static com.github.alexishuf.fastersparql.batch.base.AbstractBIt.cls2name;
import static com.github.alexishuf.fastersparql.exceptions.FSCancelledException.isCancel;

public abstract class DelegatedControlBIt<B extends Batch<B>, S extends Batch<S>> implements BIt<B> {
    private static final Logger log = LoggerFactory.getLogger(DelegatedControlBIt.class);
    private static final boolean STATS_ENABLED = FSProperties.itStats();
    public static final VarHandle OWNER;
    static {
        try {
            OWNER = MethodHandles.lookup().findVarHandle(DelegatedControlBIt.class, "plainOwner", Thread.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected BIt<S> delegate;
    @SuppressWarnings("unused") private @Nullable Thread plainOwner;
    private short lockDepth;
    private State state = ACTIVE;
    protected final BatchType<B> batchType;
    protected final Vars vars;
    protected @Nullable MetricsFeeder metrics;
    private long batches, rows;

    public DelegatedControlBIt(BIt<S> delegate, BatchType<B> batchType, Vars vars) {
        this.delegate = delegate;
        this.batchType = batchType;
        this.vars = vars;
    }

    public BIt<S> delegate() { return delegate; }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Optional.ofNullable(delegate).stream();
    }

    protected void lock() {
        Thread me = Thread.currentThread(), ac;
        int i = 0;
        while ((ac=(Thread)OWNER.compareAndExchangeAcquire(this, null, me)) != null && ac != me) {
            if ((i++&0xf) == 0xf) Thread.yield();
            else Thread.onSpinWait();
        }
        ++lockDepth;
    }
    protected void unlock() {
        if (lockDepth <= 0) throw new IllegalStateException("not locked");
        assert plainOwner == Thread.currentThread() : "not locked by this thread";
        if (--lockDepth == 0)
            OWNER.setRelease(this, null);
    }

    protected boolean isTerminated() { return state.isTerminated(); }

    protected abstract void cleanup(@Nullable Throwable cause);

    protected boolean onTermination(@Nullable Throwable error) {
        boolean cancelled = error instanceof BItCancelledException;
        lock();
        try {
            if (!isTerminated()) {
                state = cancelled ? CANCELLED : error == null ? COMPLETED : FAILED;
                try {
                    if (metrics != null)
                        metrics.completeAndDeliver(error, cancelled);
                } catch (Throwable t) {
                    log.warn("Ignoring failure to deliver metrics from {} to {}", this, metrics, t);
                }
                try {
                    cleanup(error);
                } catch (Throwable t) {
                    log.warn("Ignoring cleanup() failure for {}", this, t);
                }
                return true;
            }
        } finally {
            unlock();
        }
        log.trace("Ignoring onTermination({}, {}) on terminated {}", cancelled, error, this);
        return false;
    }

    @Override public final void close() {
        delegate.close();
        if (!isTerminated())
            onTermination(BItCancelledException.get(this));
    }

    @Override public final boolean tryCancel() {
        boolean done = delegate.tryCancel();
        if (!isTerminated())
            done &= onTermination(BItCancelledException.get(this));
        return done;
    }

    @Override public       BatchType<B> batchType() { return batchType; }
    @Override public final Vars         vars()      { return vars; }
    @Override public       State        state()     { return state; }

    @Override public @This BIt<B> metrics(@Nullable MetricsFeeder metrics) {
        this.metrics = metrics;
        if (metrics != null && delegate instanceof AbstractBIt<S> i && i.isTerminated())
            metrics.completeAndDeliver(i.error, isCancel(i.error));
        return this;
    }

    @Override public String toString() {
        return cls2name(getClass()) +'('+delegate.toString()+')';
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = selfLabel(type);
        if (type.showState())
            sb.append('[').append(state).append(']');
        sb.append('(');
        sb.append(delegate.label(StreamNodeDOT.Label.MINIMAL)).append(')');
        State delegateState;
        if (type.showState() && (delegateState=delegate.state()) != state)
            sb.append("\n delegate is ").append(delegateState);
        if (type.showStats())
            appendStatsToLabel(sb);
        return sb.toString();
    }

    private void appendStatsToLabel(StringBuilder sb) {
        var m = metrics;
        if (STATS_ENABLED || m != null) {
            long batches = this.batches, rows = this.rows;
            if (!STATS_ENABLED) {
                batches = m.batches();
                rows    = m.rows();
            }
            sb.append("\ndelivered ").append(rows);
            sb.append(" rows in ").append(batches).append(" batches");
        }
    }

    protected StringBuilder selfLabel(StreamNodeDOT.Label type) {
        return new StringBuilder().append(cls2name(getClass()));
    }

    protected void onNextBatch(Orphan<B> b) {
        var m = metrics;
        int batchRows = STATS_ENABLED || m != null ? Batch.peekTotalRows(b) : 0;
        if (STATS_ENABLED) {
            ++batches;
            rows += batchRows;
        }
        if (m != null)
            m.batch(batchRows);
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
