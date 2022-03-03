package com.github.alexishuf.fastersparql.operators.metrics;

import com.github.alexishuf.fastersparql.operators.plan.Plan;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.concurrent.TimeUnit;

@Getter @Accessors(fluent = true)
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class PlanMetrics<R> {
    private final Plan<R> plan;
    private final Class<? super R> rowClass;
    private final long totalRows;
    private final long executionStartNanos, executionEndNanos;
    private final Throwable error;
    private final boolean cancelled;

    public long duration(TimeUnit unit) {
        return unit.convert(executionEndNanos - executionStartNanos, TimeUnit.NANOSECONDS);
    }
}
