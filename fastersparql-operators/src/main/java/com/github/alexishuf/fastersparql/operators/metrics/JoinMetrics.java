package com.github.alexishuf.fastersparql.operators.metrics;

import com.github.alexishuf.fastersparql.operators.plan.Plan;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Getter @Accessors(fluent = true)
@ToString
@EqualsAndHashCode(callSuper = true)
public class JoinMetrics<R> extends PlanMetrics<R> {
    private final double leftUnmatchedRate, avgRightMatches;
    private final long maxRightMatches;

    public JoinMetrics(Plan<R> plan, Class<? super R> rowClass, long totalRows,
                       long executionStartNanos, long executionEndNanos,
                       Throwable error, boolean cancelled, double leftUnmatchedRate,
                       double avgRightMatches, long maxRightMatches) {
        super(plan, rowClass, totalRows, executionStartNanos, executionEndNanos, error, cancelled);
        this.leftUnmatchedRate = leftUnmatchedRate;
        this.avgRightMatches = avgRightMatches;
        this.maxRightMatches = maxRightMatches;
    }
}
