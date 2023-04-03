package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Objects;

public class SingletonBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private @Nullable B batch;

    public SingletonBIt(@Nullable B batch, BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
        this.batch = batch;
    }

    @Override public B nextBatch(@Nullable B b) {
        if (b != null) batchType.recycle(b);
        b = batch;
        batch = null;
        return b;
    }

    @Override public @This BIt<B> tempEager() { return this; }
    @Override public      String  toString()  { return Objects.toString(batch); }
}
