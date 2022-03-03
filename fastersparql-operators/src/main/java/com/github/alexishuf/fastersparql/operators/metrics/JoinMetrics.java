package com.github.alexishuf.fastersparql.operators.metrics;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter @Accessors(fluent = true)
@ToString
@EqualsAndHashCode(callSuper = true)
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
}
