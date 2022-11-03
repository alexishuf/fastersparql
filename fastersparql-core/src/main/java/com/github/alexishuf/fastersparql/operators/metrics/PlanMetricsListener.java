package com.github.alexishuf.fastersparql.operators.metrics;

import com.github.alexishuf.fastersparql.operators.plan.Plan;

@FunctionalInterface
public interface PlanMetricsListener {
    void accept(Plan<?, ?> plan, PlanMetrics metrics);
}
