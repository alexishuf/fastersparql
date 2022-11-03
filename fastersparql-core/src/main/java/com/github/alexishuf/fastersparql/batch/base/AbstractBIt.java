package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements trivial methods of {@link BIt} and open/closed state.
 */
public abstract class AbstractBIt<T> implements BIt<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private static final Logger log = LoggerFactory.getLogger(AbstractBIt.class);
    /** A value smaller than any {@link System#nanoTime()} call without overflow risks. */
    protected static final long ORIGIN_TIMESTAMP = System.nanoTime();

    protected long minWaitNs = 0;
    protected long maxWaitNs = 0;
    protected int minBatch = 1, maxBatch = 65_536, id = 0;
    protected boolean needsStartTime = false, closed = false, exhausted = false, clean = false;
    protected final Class<T> elementClass;
    protected final Vars vars;

    public AbstractBIt(Class<? super T> elementClass, Vars vars) {
        //noinspection unchecked
        this.elementClass = (Class<T>) elementClass;
        this.vars = vars;
    }

    /* --- --- --- abstract methods --- --- --- */

    /**
     * Releases any resources held by this iterator. Must only be called once per {@code BIt}.
     *
     * <p>This will be called in one of the following situations:</p>
     * <ul>
     *     <li>{@link BIt#close()} was called</li>
     *     <li>The last item/batch/Throwable from the {@link BIt} is being delivered</li>
     * </ul>
     *
     * @param interrupted {@code true} iff called from {@link BIt#close()}.
     */
    protected abstract void cleanup(boolean interrupted);

    /** Calls {@code cleanup()} only once after the iterator is fully consumed */
    protected void onExhausted() {
        if (exhausted) return;
        exhausted = true;
        try {
            if (!clean) {
                clean = true;
                cleanup(false);
            }
        } catch (Throwable t) {
            log.error("{}.cleanup() on exhaustion failed", this, t);
        }
    }

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

    @Override public Class<T> elementClass() { return elementClass; }

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
        if (!closed) {
            closed = true;
            if (!clean)
                cleanup(true);
        }
    }

    protected String toStringNoArgs() {
        String name = getClass().getSimpleName();
        int suffixStart = name.length() - 3;
        if (name.regionMatches(suffixStart, "BIt", 0, 3))
            return name.substring(0, suffixStart);
        return name+'@'+id();
    }

    @Override public String toString() { return toStringNoArgs(); }

    protected String toStringWithOperands(Collection<?> operands) {
        var sb = new StringBuilder(200).append(toStringNoArgs());
        int taken = 0, n = operands.size();
        for (var i = operands.iterator(); sb.length() < 160 && i.hasNext(); ++taken)
            sb.append(i.next()).append(", ");
        if (taken < n)
            sb.append("...");
        else
            sb.setLength(sb.length()-2);
        return sb.toString();
    }
}
