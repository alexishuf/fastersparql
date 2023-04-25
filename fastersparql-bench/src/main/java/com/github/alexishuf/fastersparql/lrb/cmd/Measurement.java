package com.github.alexishuf.fastersparql.lrb.cmd;

import com.github.alexishuf.fastersparql.fed.FedMetrics;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import org.checkerframework.checker.nullness.qual.Nullable;

public record Measurement(MeasureTask task, int rep,
                          long dispatchNs, long selectionAndAgglutinationNs, long optimizationNs,
                          long firstRowNs, long allRowsNs, int rows,
                          long minNsBetweenBatches, long maxNsBetweenBatches, long terminalNs,
                          boolean cancelled, @Nullable String error) {
    public Measurement(MeasureTask task, int rep, @Nullable FedMetrics fed,
                       @Nullable Metrics plan,
                       @Nullable Throwable error) {
        this(task, rep, fed == null ? 0 : fed.dispatchNs,
                fed == null ? 0 : fed.selectionAndAgglutinationNs,
                fed == null ? 0 : fed.optimizationNs,
                plan == null ? 0 : plan.firstRowNanos,
                plan == null ? 0 : plan.allRowsNanos,
                plan == null ? 0 : (int)plan.rows,
                plan == null ? 0 : plan.minNanosBetweenBatches,
                plan == null ? 0 : plan.maxNanosBetweenBatches,
                plan == null ? 0 : plan.terminalNanos,
                plan != null && plan.cancelled,
                plan == null || plan.error == null ? (error == null ? null : error.toString())
                                                   : plan.error.toString());
    }
}
