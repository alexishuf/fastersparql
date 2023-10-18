package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.AbstractStage;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import static com.github.alexishuf.fastersparql.batch.type.Batch.asPooled;
import static com.github.alexishuf.fastersparql.batch.type.Batch.asUnpooled;

public class ConverterStage<I extends Batch<I>, O extends  Batch<O>> extends AbstractStage<I, O> {
    protected final int cols;
    protected @Nullable O recycled;

    public ConverterStage(BatchType<O> type, Emitter<I> upstream) {
        super(type, upstream.vars());
        cols = vars.size();
        subscribeTo(upstream);
    }

    @Override public String toString() {
        return String.format("%s@%x<-%s",
                batchType.getClass().getSimpleName().replace("BatchType", ""),
                System.identityHashCode(this),
                upstream);
    }

    @Override public @This ConverterStage<I, O> subscribeTo(Emitter<I> emitter) {
        if (!emitter.vars().equals(vars))
            throw new IllegalArgumentException("Mismatching vars");
        super.subscribeTo(emitter);
        return this;
    }

    @Override public @Nullable I onBatch(I batch) {
        if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(batch);
        int rows = batch == null ? 0 : batch.rows;
        if (rows == 0) return batch;
        O o = batchType.empty(asUnpooled(recycled), batch.rows, cols);
        recycled = null;
        recycled = asPooled(downstream.onBatch(o.putConverting(batch)));
        return batch;
    }

    @Override public void onRow(I batch, int row) {
        if (EmitterStats.ENABLED && stats != null) stats.onRowPassThrough();
        if (batch == null) return;
        O o = batchType.emptyForTerms(asUnpooled(recycled), cols, cols);
        recycled = null;
        recycled = asPooled(downstream.onBatch(o.putRowConverting(batch, row)));
    }
}
