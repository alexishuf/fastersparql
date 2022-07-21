package com.github.alexishuf.fastersparql.operators.metrics;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class PlanMetrics {
    protected final String planName;
    protected final long totalRows;
    protected final long executionStartNanos, executionEndNanos;
    protected final Throwable error;
    protected final boolean cancelled;

    public PlanMetrics(String planName, long totalRows, long executionStartNanos,
                   Throwable error, boolean cancelled) {
        this(planName, totalRows, executionStartNanos, System.nanoTime(), error, cancelled);
    }

    public PlanMetrics(String planName, long totalRows, long executionStartNanos,
                       long executionEndNanos, Throwable error, boolean cancelled) {
        this.planName = planName;
        this.totalRows = totalRows;
        this.executionStartNanos = executionStartNanos;
        this.executionEndNanos = executionEndNanos;
        this.error = error;
        this.cancelled = cancelled;
    }

    /* --- ---- ---- getters --- ---- --- */

    public String planName() { return planName; }
    public long totalRows() { return totalRows; }
    public long executionStartNanos() { return executionStartNanos; }
    public long executionEndNanos() { return executionEndNanos; }
    public Throwable error() { return error; }
    public boolean cancelled() { return cancelled; }

    public long duration(TimeUnit unit) {
        return unit.convert(executionEndNanos - executionStartNanos, TimeUnit.NANOSECONDS);
    }

    /* --- ---- ---- java.lang.Object methods --- ---- --- */

    @Override public String toString() {
        return "PlanMetrics{" +
                "planName='" + planName + '\'' +
                ", totalRows=" + totalRows +
                ", executionStartNanos=" + executionStartNanos +
                ", executionEndNanos=" + executionEndNanos +
                ", error=" + error +
                ", cancelled=" + cancelled +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PlanMetrics)) return false;
        PlanMetrics that = (PlanMetrics) o;
        return totalRows == that.totalRows
                && executionStartNanos == that.executionStartNanos
                && executionEndNanos == that.executionEndNanos
                && cancelled == that.cancelled
                && planName.equals(that.planName)
                && Objects.equals(error, that.error);
    }

    @Override public int hashCode() {
        return Objects.hash(planName, totalRows, executionStartNanos, executionEndNanos, error, cancelled);
    }
}
