package com.github.alexishuf.fastersparql.util.concurrent;

public interface StatsPool {
    /** An identifiable and reasonably short name for the pool instance. */
    String name();

    /** Number of objects pooled which have no defined thread affinity. */
    int sharedObjects();

    /**
     * Estimated memory usage, in bytes, of all objects pooled that have no thread affinity.
     */
    int sharedBytes();

    /** Number of objects pooled that <strong>HAVE</strong> a thread affinity. */
    int localObjects();

    /**
     * Estimated memory usage, in bytes, of all pooled objects that <strong>HAVE</strong>
     * a thread affinity.
     */
    int localBytes();
}
