package com.github.alexishuf.fastersparql.operators.metrics;

import java.util.Objects;

public class JoinMetrics extends PlanMetrics {
    private final double leftUnmatchedRate, avgRightMatches;
    private final long leftRows, maxRightMatches;

    public JoinMetrics(String planName,  long totalRows, long executionStartNanos,
                       long executionEndNanos, Throwable error, boolean cancelled,
                       long leftRows, double leftUnmatchedRate, double avgRightMatches,
                       long maxRightMatches) {
        super(planName, totalRows, executionStartNanos, executionEndNanos, error, cancelled);
        this.leftRows = leftRows;
        this.leftUnmatchedRate = leftUnmatchedRate;
        this.avgRightMatches = avgRightMatches;
        this.maxRightMatches = maxRightMatches;
    }

    /* --- ---- ---- getters --- ---- --- */

    public double leftUnmatchedRate() { return leftUnmatchedRate; }
    public double avgRightMatches() { return avgRightMatches; }
    public long leftRows() { return leftRows; }
    public long maxRightMatches() { return maxRightMatches; }

    /* --- ---- ---- java.lang.Object methods --- ---- --- */

    @Override public String toString() {
        return "JoinMetrics{" +
                "leftUnmatchedRate=" + leftUnmatchedRate +
                ", avgRightMatches=" + avgRightMatches +
                ", leftRows=" + leftRows +
                ", maxRightMatches=" + maxRightMatches +
                ", planName='" + planName + '\'' +
                ", totalRows=" + totalRows +
                ", executionStartNanos=" + executionStartNanos +
                ", executionEndNanos=" + executionEndNanos +
                ", error=" + error +
                ", cancelled=" + cancelled +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinMetrics)) return false;
        if (!super.equals(o)) return false;
        JoinMetrics that = (JoinMetrics) o;
        return Double.compare(that.leftUnmatchedRate, leftUnmatchedRate) == 0
                && Double.compare(that.avgRightMatches, avgRightMatches) == 0
                && leftRows == that.leftRows
                && maxRightMatches == that.maxRightMatches;
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), leftUnmatchedRate, avgRightMatches, leftRows, maxRightMatches);
    }
}
