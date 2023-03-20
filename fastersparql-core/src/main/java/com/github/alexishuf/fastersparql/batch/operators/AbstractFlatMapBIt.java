package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public abstract class AbstractFlatMapBIt<B extends Batch<B>> extends AbstractBIt<B> {
    protected @Nullable BIt<B> inner;

    public AbstractFlatMapBIt(BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
    }

    @Override @This public BIt<B> minWait(long time, TimeUnit unit) {
        if (inner != null) inner.minWait(time, unit);
        return super.minWait(time, unit);
    }

    @Override @This public BIt<B> minWait(Duration duration) {
        if (inner != null) inner.minWait(duration);
        return super.minWait(duration);
    }

    @Override @This public BIt<B> maxWait(long time, TimeUnit unit) {
        if (inner != null) inner.maxWait(time, unit);
        return super.maxWait(time, unit);
    }

    @Override @This public BIt<B> maxWait(Duration duration) {
        if (inner != null) inner.maxWait(duration);
        return super.maxWait(duration);
    }

    @Override public BIt<B> minBatch(int rows) {
        if (inner != null) inner.minBatch(rows);
        return super.minBatch(rows);
    }

    @Override @This public BIt<B> maxBatch(int size) {
        if (inner != null) inner.maxBatch(size);
        return super.maxBatch(size);
    }

    @Override public @Nullable B recycle(B batch) {
        if (batch != null && (inner == null || inner.recycle(batch) != null))
            return super.recycle(batch);
        return null;
    }

    @Override public @This BIt<B> tempEager() {
        if (inner != null) inner.tempEager();
        return super.tempEager();
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        if (cause != null && inner != null) inner.close();
    }
}
