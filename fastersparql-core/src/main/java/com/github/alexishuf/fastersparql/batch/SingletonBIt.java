package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Objects;

public class SingletonBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private final @Nullable B batch;
    private boolean ended;

    public SingletonBIt(@Nullable B batch, BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
        this.batch = batch;
    }

    @Override public B nextBatch(@Nullable B b) {
        if (b != null) batchType.recycle(b);
        if (ended) return null;
        ended = true;
        return batch;
    }

    @Override public @This BIt<B> tempEager() { return this; }
    @Override public      String  toString()  { return Objects.toString(batch); }
}
