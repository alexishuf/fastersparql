package com.github.alexishuf.fastersparql.operators.metrics;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface MetricsFeeder {
    /** Records that a batch of {@code n} rows was produced. */
    void batch(int n);

    /** Number of previous {@link #batch(int)} calls */
    long batches();

    /** Sum of all {@code n} in previous {@link #batch(int)} calls */
    long rows();

    /**
     * Signals that no further events will happen for this {@link MetricsFeeder}, performs any
     * required processing on the {@link Metrics} object being built and delivers it to
     * registered listeners.
     */
    void completeAndDeliver(@Nullable Throwable cause, boolean cancelled);
}
