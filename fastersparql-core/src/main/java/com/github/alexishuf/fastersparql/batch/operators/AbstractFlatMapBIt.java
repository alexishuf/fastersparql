package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public abstract class AbstractFlatMapBIt<B extends Batch<B>> extends AbstractBIt<B> {
    protected @NonNull BIt<B> inner;

    public AbstractFlatMapBIt(Vars vars, @NonNull BIt<B> inner) {
        super(inner.batchType(), vars);
        this.inner = inner;
    }

    @Override @This public BIt<B> minWait(long time, TimeUnit unit) {
        inner.minWait(time, unit);
        return super.minWait(time, unit);
    }

    @Override @This public BIt<B> minWait(Duration duration) {
        inner.minWait(duration);
        return super.minWait(duration);
    }

    @Override @This public BIt<B> maxWait(long time, TimeUnit unit) {
        inner.maxWait(time, unit);
        return super.maxWait(time, unit);
    }

    @Override @This public BIt<B> maxWait(Duration duration) {
        inner.maxWait(duration);
        return super.maxWait(duration);
    }

    @Override public BIt<B> minBatch(int rows) {
        inner.minBatch(rows);
        return super.minBatch(rows);
    }

    @Override @This public BIt<B> maxBatch(int size) {
        inner.maxBatch(size);
        return super.maxBatch(size);
    }

    @Override public @This BIt<B> tempEager() {
        inner.tempEager();
        return super.tempEager();
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        try {
            inner.close();
        } catch (Throwable t) { reportCleanupError(t); }
    }
}
