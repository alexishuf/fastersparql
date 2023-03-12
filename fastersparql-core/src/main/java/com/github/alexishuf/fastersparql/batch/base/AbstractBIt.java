package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItClosedAtException;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements trivial methods of {@link BIt} and open/closed state.
 */
public abstract class AbstractBIt<T> implements BIt<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private static final Logger log = LoggerFactory.getLogger(AbstractBIt.class);
    private static final boolean IS_DEBUG_ENABLED = log.isDebugEnabled();
    /** A value smaller than any {@link System#nanoTime()} call without overflow risks. */
    protected static final long ORIGIN_TIMESTAMP = System.nanoTime();

    protected long minWaitNs = 0;
    protected long maxWaitNs = 0;
    protected int minBatch = 1, maxBatch = BIt.DEF_MAX_BATCH, id = 0;
    protected boolean needsStartTime = false, closed = false, clean = false;
    protected @MonotonicNonNull Throwable error;
    protected final RowType<T> rowType;
    protected final Vars vars;

    public AbstractBIt(RowType<T> rowType, Vars vars) {
        this.rowType = rowType;
        this.vars = vars;
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
    protected void cleanup(@Nullable Throwable cause) { /* do nothing by default */ }

    protected final void checkError() {
        if (error == null) return;
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
     *         {@link BufferedBIt#feed(Object)} has completed or threw {@code cause})</li>
     *     <li>The {@link BIt} was fully consumed (i.e., {@link BIt#hasNext()} {@code == false} and
     *         empty {@link BIt#nextBatch()}</li>
     *     <li>{@link BIt#close()} was called ({@code cause} will be
     *         a {@link BItClosedAtException})</li>
     * </ul>
     *
     * @param cause if non-null, this is the exception that caused the termination.
     */
    protected final void onTermination(@Nullable Throwable cause) {
        var msg = cause == null ? "null" : cause.getClass().getSimpleName() + ": " + cause.getMessage();
        if (clean) {
            if (cause != null && !(cause instanceof BItClosedAtException)) {
                if (error == null)
                    log.info(ON_TERM_TPL_PREV, this, msg, "null");
                else
                    log.debug(ON_TERM_TPL_PREV, this, msg, Objects.toString(this.error));
            } else {
                log.trace(ON_TERM_TPL_PREV, this, msg, Objects.toString(this.error));
            }
        }
        clean = true;
        error = cause;
        if (cause == null || cause instanceof BItClosedAtException) {
            log.trace(ON_TERM_TPL, this, msg);
        } else if (cause instanceof FSCancelledException) {
            log.debug(ON_TERM_TPL, this, msg);
        } else if (IS_DEBUG_ENABLED) {
            log.debug(ON_TERM_TPL, this, msg, cause);
        } else {
            log.info(ON_TERM_TPL, this, msg);
        }
        try {
            cleanup(cause);
        } catch (Throwable t) {
            log.error("{}.cleanup() on exhaustion failed", this, t);
        }
    }
    private static final String ON_TERM_TPL_PREV = "{}.onTermination({}) ignored: previous onTermination({})";
    private static final String ON_TERM_TPL = "{}.onTermination({})";

    /* --- --- --- helpers --- --- --- */

    public boolean isClosed() { return closed; }

    /**
     * Tests whether a batch with {@code size} items started when {@link System#nanoTime()}
     * was {@code start} is ready.
     *
     * <p>A batch is ready when it is not empty and one of the following hold:</p>
     *
     * <ol>
     *     <li>The max size has been reached: {@code size >= maxBatch()}</li>
     *     <li>The maximum wait time has been reached:
     *         {@code nanoTime()-start >= maxWait(NANOSECONDS)}</li>
     *     <li>The minimum size has been reached after the minimum wait:
     *         {@code size >= minBatch() && nanoTime()-start >= minWait(NANOSECONDS}</li>
     * </ol>
     *
     * @param size the current batch size.
     * @param start when the batch started being built.
     * @return {@code true} iff the batch is ready as {@link BIt#minBatch()},
     *         {@link BIt#maxBatch()}, {@link BIt#minWait(TimeUnit)} and
     *         {@link BIt#maxWait(TimeUnit)}.
     */
    protected boolean ready(int size, long start) {
        if (size >= maxBatch) return true;
        else if (size == 0) return false;
        else if ((minWaitNs == 0 && (size >= minBatch || maxWaitNs == 0))) return true;
        long elapsed = System.nanoTime() - start;
        return (elapsed > minWaitNs && size >= minBatch) || (elapsed >= maxWaitNs);
    }

    protected int id() { return id == 0 ? id = nextId.getAndIncrement() : id; }

    /* --- --- --- implementations --- --- --- */

    @Override public RowType<T> rowType() { return rowType; }

    @Override public Vars vars() { return vars; }

    @Override public BIt<T> minWait(long time, TimeUnit unit) {
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
        return this;
    }

    @Override public long minWait(TimeUnit unit) {
        return unit.convert(minWaitNs, TimeUnit.NANOSECONDS);
    }

    @Override public BIt<T> maxWait(long time, TimeUnit unit) {
        if (time < 0) {
            assert false : "negative time";
            log.warn("{}.maxWait({}, {}): treating negative as default (0)", this, time, unit);
            time = 0;
        }
        needsStartTime = (minWaitNs > 0 && minWaitNs != Long.MAX_VALUE)
                      || (time      > 0 && time      != Long.MAX_VALUE);
        maxWaitNs = unit.toNanos(time);
        return this;
    }

    @Override public long maxWait(TimeUnit unit) {
        return unit.convert(maxWaitNs, TimeUnit.NANOSECONDS);
    }

    @Override public BIt<T> minBatch(int size) {
        if (size < 0) {
            log.warn("{}.minBatch({}): treating negative size as 0", this, size);
            size = 0;
        }
        minBatch = size;
        return this;
    }

    @Override public int minBatch() {
        return minBatch;
    }

    @Override public BIt<T> maxBatch(int size) {
        if (size < 1)
            throw new IllegalArgumentException(this+".maxBatch("+size+"): expected > 0");
        maxBatch = size;
        return this;
    }

    @Override public int maxBatch() {
        return maxBatch;
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        if (!clean) // close() before onExhausted()
            onTermination(new BItClosedAtException(this));
    }

    protected String toStringNoArgs() {
        String name = getClass().getSimpleName();
        int suffixStart = name.length() - 3;
        if (name.regionMatches(suffixStart, "BIt", 0, 3))
            name = name.substring(0, suffixStart);
        return name+'@'+id();
    }

    @Override public String toString() { return toStringNoArgs(); }

    protected String toStringWithOperands(Collection<?> operands) {
        var sb = new StringBuilder(200).append(toStringNoArgs()).append('[');
        int taken = 0, n = operands.size();
        for (var i = operands.iterator(); sb.length() < 160 && i.hasNext(); ++taken)
            sb.append(i.next()).append(", ");
        if (taken < n)
            sb.append("...");
        else
            sb.setLength(sb.length()-2);
        return sb.append(']').toString();
    }
}
