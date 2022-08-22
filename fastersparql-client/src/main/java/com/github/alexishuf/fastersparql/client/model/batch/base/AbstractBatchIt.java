package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.BatchIt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Implements trivial methods of {@link BatchIt} and open/closed state */
public abstract class AbstractBatchIt<T> implements BatchIt<T> {
    private static final Logger log = LoggerFactory.getLogger(AbstractBatchIt.class);

    private long minWaitNs = 0;
    protected int minBatch = 1, maxBatch = 65_536;
    private boolean closed = false;
    protected final Class<T> elementClass;
    protected final String name;

    public AbstractBatchIt(Class<T> elementClass, String name) {
        this.elementClass = elementClass;
        this.name = name;
    }


    /* --- --- --- abstract methods --- --- --- */

    /** Releases any resources held by this iterator. Implementations must be idempotent */
    protected abstract void cleanup();

    /* --- --- --- helper methods --- --- --- */

    /** Whether {@link BatchIt#close()} has been called */
    protected boolean isClosed() { return closed; }

    /** Throw if {@code close()}d */
    protected void checkOpen() {
        if (closed)
            throw new IllegalStateException(this+" is close()d, cannot consume");
    }

    /* --- --- --- implementations --- --- --- */

    @Override public Class<T> elementClass() { return elementClass; }

    @Override public BatchIt<T> minWait(long time, TimeUnit unit) {
        if (time < 0) {
            log.warn("{}.minWait({}, {}): treating negative time as 0.", this, time, unit);
            time = 0;
        }
        minWaitNs = unit.toNanos(time);
        return this;
    }

    @Override public long minWait(TimeUnit unit) {
        return unit.convert(minWaitNs, TimeUnit.NANOSECONDS);
    }

    @Override public BatchIt<T> minBatch(int size) {
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

    @Override public BatchIt<T> maxBatch(int size) {
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
            cleanup();
        }
    }

    @Override public String toString() {
        return name;
    }
}
