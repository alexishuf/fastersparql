package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.*;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.lang.invoke.MethodHandles.lookup;

/**
 * Implements trivial methods of {@link BIt} and open/closed state.
 */
public abstract class AbstractBIt<B extends Batch<B>> implements BIt<B> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private static final Logger log = LoggerFactory.getLogger(AbstractBIt.class);
    private static final boolean IS_DEBUG_ENABLED = log.isDebugEnabled();
    protected static final VarHandle STATE;

    static {
        try {
            STATE = lookup().findVarHandle(AbstractBIt.class, "plainState", State.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected long minWaitNs;
    protected long maxWaitNs;
    protected int minBatch = 1, maxBatch = BIt.DEF_MAX_BATCH, id = 0;
    @SuppressWarnings("CanBeFinal") protected State plainState = State.ACTIVE;
    protected boolean needsStartTime = false, eager = false;
    protected final short nColumns;
    protected @MonotonicNonNull Throwable error;
    protected final BatchType<B> batchType;
    protected @Nullable MetricsFeeder metrics;
    protected final Vars vars;

    public AbstractBIt(BatchType<B> batchType, Vars vars) {
        this.batchType = batchType;
        this.vars = vars;
        int nColumns = vars.size();
        if (nColumns > Short.MAX_VALUE)
            throw new IllegalArgumentException("columns too large");
        this.nColumns = (short) nColumns;
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.empty();
    }

    /* --- --- --- abstract methods --- --- --- */

    /**
     * Releases any resources held by this iterator. This will only be called once per {@link BIt}.
     *
     * <p>Implementors of {@link AbstractBIt} MUST not call this, they should call
     * {@link AbstractBIt#onTermination(Throwable)} instead, which is idempotent and will invoke this
     * on its first call.</p>
     *
     * <p>This method will be called once one of three things happens:</p>
     *
     * <ul>
     *     <li>Normal completion: all values of the iterator have been delivered downstream</li>
     *     <li>Error: iteration was stopped due to an error upstream on within this iterator</li>
     *     <li>Cancellation: {@link BIt#close()} was called before a normal or error completion</li>
     * </ul>
     *
     * @param cause {@code null} in case of <i>normal completion</i>, the error in case of
     *              <i>error completion</i>, or a {@link BItClosedAtException} for {@code this}
     *              in case of <i>cancellation</i>
     */
    protected void cleanup(@Nullable Throwable cause) {}

    protected final void checkError() {
        if (STATE.getAcquire(this) == State.ACTIVE || error == null) return;
        if (error instanceof BItClosedAtException e)
            throw new BItReadClosedException(this, e.asFor(this));
        throw new BItReadFailedException(this, error);
    }

    /**
     * Delegates to {@link AbstractBIt#cleanup(Throwable)}, but only on the first call.
     *
     * <p>This method should be called when one of the following happens:</p>
     * <ul>
     *     <li>The source that feeds this {@link BIt} has been exhausted (e.g., a thread calling
     *         {@link CallbackBIt#offer(Batch)} has completed or threw {@code cause})</li>
     *     <li>The {@link BIt} was fully consumed (i.e., {@link BIt#nextBatch(B)} {@code == null}</li>
     *     <li>{@link BIt#close()} was called ({@code cause} will be
     *         a {@link BItClosedAtException})</li>
     * </ul>
     *
     * @param cause if non-null, this is the exception that caused the termination.
     */
    protected final void onTermination(@Nullable Throwable cause) {
        State tgt = cause == null ? State.COMPLETED
                : (cause instanceof BItClosedAtException ? State.CANCELLED : State.FAILED);
        if (STATE.compareAndExchangeRelease(this, State.ACTIVE, tgt) != State.ACTIVE) {
            if (tgt == State.FAILED) {
                if (error == null)
                    log.info(ON_TERM_TPL_PREV, this, cause, "null");
                else
                    log.debug(ON_TERM_TPL_PREV, this, cause, Objects.toString(this.error));
            } else {
                log.trace(ON_TERM_TPL_PREV, this, cause, Objects.toString(this.error));
            }
            return;
        }
        error = cause;
        switch (tgt) {
            case COMPLETED -> log.trace(ON_TERM_TPL, this, "");
            case CANCELLED -> log.trace(ON_TERM_TPL, this, "close()/cancelled");
            case FAILED    -> {
                String msg = cause.toString();
                if (IS_DEBUG_ENABLED && !FSCancelledException.isCancel(cause))
                    log.debug(ON_TERM_TPL, this, msg, cause);
                else
                    log.info(ON_TERM_TPL, this, msg);
            }
        }
        try {
            if (metrics != null)
                metrics.completeAndDeliver(cause, tgt == State.CANCELLED);
        } catch (Throwable t) {
            log.error("{}.metrics.completeAndDeliver({}) failed", this, cause, t);
        }
        try {
            cleanup(cause);
        } catch (Throwable t) {
            log.error("{}.cleanup() failed", this, t);
        }
    }
    private static final String ON_TERM_TPL_PREV = "{}.onTermination({}) ignored: previous onTermination({})";
    private static final String ON_TERM_TPL = "{}.onTermination({})";

    /* --- --- --- helpers --- --- --- */

    public State state() { return (State)STATE.getAcquire(this); }

    /**
     * This will be called after {@link BIt#minBatch(int)}, {@link BIt#maxBatch(int)},
     * {@link BIt#minWait(long, TimeUnit)} or {@link BIt#maxWait(long, TimeUnit)} (and related
     * setter overrides) are called.
     *
     * <p>Implementations should use this to updated derived fields or to complete
     * filling but not yet ready batches.</p>
     */
    protected void updatedBatchConstraints() {}

    protected final void onNextBatch(@Nullable B b) {
        if (b == null) return;
        var m = metrics;
        if (m != null) m.batch(b.totalRows());
        eager = false;
    }

    /**
     * Get how many nanoseconds to wait to check again whether a batch with {@code r} rows
     * that started filling when {@link System#nanoTime()} was {@code start} is ready.
     *
     * <p>If the batch is ready, this returns 0. If this iterator is not configured for time-based
     * batch readiness ({@link BIt#minWait(long, TimeUnit)} and
     * {@link BIt#maxWait(long, TimeUnit)}), returns {@link Long#MAX_VALUE}. </p>
     *
     * @param r current batch size
     * @param start when the batch started filling (can be {@link Timestamp#ORIGIN})
     * @return Zero if batch is ready, {@link Long#MAX_VALUE} if the iterator does not have
     *         time-based batch readiness, Else, nanoseconds until time until
     *         {@link BIt#minWait(TimeUnit)} or {@link BIt#maxWait(TimeUnit)}.
     */
    protected long readyInNanos(int r, long start) {
        if      (r == 0                  ) return Long.MAX_VALUE;
        else if (r >= maxBatch   || eager) return 0;
        else if (!needsStartTime         ) return r >= minBatch ? 0 : Long.MAX_VALUE;

        long elapsed = Timestamp.nanoTime() - start;
        if      (elapsed <  minWaitNs) return Math.max(0, minWaitNs-elapsed);
        else if (r       >=  minBatch) return 0;
        else if (elapsed <  maxWaitNs) return Math.max(0, maxWaitNs-elapsed);
        else if (r       >          0) return 0;
        else                           return Long.MAX_VALUE;
    }


    protected int id() { return id == 0 ? id = nextId.getAndIncrement() : id; }

    /* --- --- --- implementations --- --- --- */

    @Override public BatchType<B> batchType() { return batchType; }

    @Override public Vars vars() { return vars; }

    @Override public @This BIt<B> metrics(@Nullable MetricsFeeder metrics) {
        this.metrics = metrics;
        State s = state();
        if (s.isTerminated() && metrics != null)
            metrics.completeAndDeliver(error,  s == State.CANCELLED);
        return this;
    }

    @Override public BIt<B> preferred() {
        needsStartTime = true;
        minWaitNs = FSProperties.batchMinWait(TimeUnit.NANOSECONDS);
        maxWaitNs = FSProperties.batchMaxWait(TimeUnit.NANOSECONDS);
        minBatch  = FSProperties.batchMinSize();
        maxBatch = Math.max(minBatch, DEF_MAX_BATCH);
        updatedBatchConstraints();
        return this;
    }

    @Override public BIt<B> quickWait() {
        minWaitNs = QUICK_MIN_WAIT_NS;
        maxWaitNs = QUICK_MAX_WAIT_NS;
        updatedBatchConstraints();
        return this;
    }

    @Override public @This BIt<B> eager() {
        needsStartTime = false;
        minWaitNs = maxWaitNs = 0;
        minBatch = 1;
        updatedBatchConstraints();
        return this;
    }

    @Override public BIt<B> minWait(long time, TimeUnit unit) {
        if (time < 0) {
            assert false : "negative time";
            log.warn("{}.minWait({}, {}): treating negative as default (0)", this, time, unit);
            time = 0;
        }
        needsStartTime = (maxWaitNs > 0 && maxWaitNs != Long.MAX_VALUE)
                      || (time      > 0 && time      != Long.MAX_VALUE);
        if (time > 0 && maxWaitNs == 0)
            maxWaitNs = Long.MAX_VALUE;
        minWaitNs = unit.toNanos(time);
        updatedBatchConstraints();
        return this;
    }

    @Override public long minWait(TimeUnit unit) {
        return unit.convert(minWaitNs, TimeUnit.NANOSECONDS);
    }

    @Override public BIt<B> maxWait(long time, TimeUnit unit) {
        if (time < 0) {
            assert false : "negative time";
            log.warn("{}.maxWait({}, {}): treating negative as default (0)", this, time, unit);
            time = 0;
        }
        needsStartTime = (minWaitNs > 0 && minWaitNs != Long.MAX_VALUE)
                      || (time      > 0 && time      != Long.MAX_VALUE);
        maxWaitNs = unit.toNanos(time);
        updatedBatchConstraints();
        return this;
    }

    @Override public long maxWait(TimeUnit unit) {
        return unit.convert(maxWaitNs, TimeUnit.NANOSECONDS);
    }

    @Override public BIt<B> minBatch(int rows) {
        if (rows < 0) {
            log.warn("{}.minBatch({}): treating negative size as 0", this, rows);
            rows = 0;
        }
        minBatch = rows;
        updatedBatchConstraints();
        return this;
    }

    @Override public final int minBatch() { return minBatch; }

    @Override public BIt<B> maxBatch(int size) {
        if (size < 1)
            throw new IllegalArgumentException(this+".maxBatch("+size+"): expected > 0");
        maxBatch = size;
        updatedBatchConstraints();
        return this;
    }

    @Override public final int maxBatch() { return maxBatch; }

    @Override public @This BIt<B> tempEager() {
        eager = true;
        updatedBatchConstraints();
        return this;
    }

    @Override public void close() {
        if (state() == State.ACTIVE)
            onTermination(new BItClosedAtException(this));
    }

    protected String toStringNoArgs() {
        return cls2name(getClass())+'@'+id();
    }

    static String cls2name(Class<?> cls) {
        String name = cls.getSimpleName();
        int suffixStart = name.length() - 3;
        if (name.regionMatches(suffixStart, "BIt", 0, 3))
            name = name.substring(0, suffixStart);
        return name;
    }

    @Override public String toString() { return toStringNoArgs(); }

    protected String toStringWithOperands(Collection<?> operands) {
        int taken = 0, n = operands.size();
        if (n == 0)
            return toStringNoArgs()+"()";
        if (n == 1)
            return toStringNoArgs()+"("+operands.iterator().next()+")";
        var sb = new ByteRope(512).append(toStringNoArgs()).append('(');
        for (var i = operands.iterator(); sb.length() < 508 && i.hasNext(); ++taken)
            sb.append("\n  ").append(i.next());
        if (taken < n)
            sb.append("\n  ...");
        return sb.append("\n)").toString();
    }
}
