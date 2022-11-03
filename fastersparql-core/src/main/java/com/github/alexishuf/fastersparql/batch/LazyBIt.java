package com.github.alexishuf.fastersparql.batch;

public interface LazyBIt<T> extends BIt<T> {
    /**
     * Starts the generator associated to this {@link BIt} if it has not already started.
     *
     * <p>This operation is idempotent and thread-safe.</p>
     *
     * <p>This method is implicitly called when the {@link BIt} begins iteration via
     * {@link BIt#hasNext()}/{@link BIt#nextBatch()} and similar methods.</p>
     */
    void start();
}
