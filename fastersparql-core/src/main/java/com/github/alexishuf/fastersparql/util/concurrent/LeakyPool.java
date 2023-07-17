package com.github.alexishuf.fastersparql.util.concurrent;

public interface LeakyPool {
    /**
     * A pool may keep references to objects that already left the pool. This
     * method finds and sets such references to {@code null}.
     *
     * <p>Implementations must be thread-safe: this can be called from any thread
     * concurrently with calls to offer/get methods and with other calls to this method.</p>
     */
    void cleanLeakyRefs();
}
