package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.AbstractStage;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.exceptions.NoDownstreamException;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;

public sealed abstract class MetricsStage<B extends Batch<B>>
        extends AbstractStage<B, B, MetricsStage<B>> {
    private static final StaticMethodOwner CONSTRUCTOR = new StaticMethodOwner("MetricsStage.init");
    private final Metrics metrics;

    public static <B extends Batch<B>> Orphan<MetricsStage<B>>
    create(Orphan<? extends Emitter<B, ?>> orphanUpstream, Metrics metrics) {
        return new Concrete<>(orphanUpstream.takeOwnership(CONSTRUCTOR), metrics);
    }

    private MetricsStage(Emitter<B, ?> upstream, Metrics metrics) {
        super(upstream.batchType(), upstream.vars());
        this.metrics = metrics;
        subscribeTo(upstream.releaseOwnership(CONSTRUCTOR));
    }

    private static final class Concrete<B extends Batch<B>> extends MetricsStage<B>
            implements Orphan<MetricsStage<B>> {
        public Concrete(Emitter<B, ?> upstream, Metrics metrics) {
            super(upstream, metrics);
        }
        @Override public MetricsStage<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    @Override public void onBatch(Orphan<B> batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        if (batch != null) {
            if (downstream == null) throw new NoDownstreamException(this);
            metrics.batch(Batch.peekTotalRows(batch));
            downstream.onBatch(batch);
        }
    }
    @Override public void onBatchByCopy(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        if (batch != null) {
            if (downstream == null) throw new NoDownstreamException(this);
            metrics.batch(batch.totalRows());
            downstream.onBatchByCopy(batch);
        }
    }

    @Override public void onComplete() {
        metrics.completeAndDeliver(null, false);
        super.onComplete();
    }

    @Override public void onError(Throwable cause) {
        metrics.completeAndDeliver(cause, false);
        super.onError(cause);
    }

    @Override public void onCancelled() {
        metrics.completeAndDeliver(null, true);
        super.onCancelled();
    }
}
