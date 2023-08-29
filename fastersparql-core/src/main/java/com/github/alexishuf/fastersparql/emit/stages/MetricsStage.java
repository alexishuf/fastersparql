package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.AbstractStage;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.exceptions.NoDownstreamException;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class MetricsStage<B extends Batch<B>> extends AbstractStage<B, B> {
    private final Metrics metrics;
    private int rebindAcquired;

    public MetricsStage(Emitter<B> upstream, Metrics metrics) {
        super(upstream.batchType(), upstream.vars());
        this.metrics = metrics;
        subscribeTo(upstream);
    }

    @Override public void rebindAcquire() {
        ++rebindAcquired;
        super.rebindAcquire();
    }

    @Override public void rebindRelease() {
        --rebindAcquired;
        super.rebindRelease();
    }

    @Override public @Nullable B onBatch(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        if (batch == null) return null;
        if (downstream == null) throw new NoDownstreamException(this);
        metrics.batch(batch.rows);
        return downstream.onBatch(batch);
    }

    @Override public void onComplete() {
        if (rebindAcquired == 0)
            metrics.completeAndDeliver(null, false);
        super.onComplete();
    }

    @Override public void onError(Throwable cause) {
        if (rebindAcquired == 0)
            metrics.completeAndDeliver(cause, false);
        super.onError(cause);
    }

    @Override public void onCancelled() {
        if (rebindAcquired == 0)
            metrics.completeAndDeliver(null, true);
        super.onCancelled();
    }
}
