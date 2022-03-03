package com.github.alexishuf.fastersparql.operators.metrics;

@FunctionalInterface
public interface PlanMetricsListener {
    void accept(PlanMetrics<?> metrics);
}
