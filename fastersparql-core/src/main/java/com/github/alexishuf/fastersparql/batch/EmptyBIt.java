package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

public final class EmptyBIt<B extends Batch<B>> extends AbstractBIt<B> {
    public EmptyBIt(BatchType<B> batchType, Vars vars) { super(batchType, vars); }

    @Override public B            nextBatch(@Nullable B b) {
        batchType.recycle(b);
        return null;
    }
    @Override public @This BIt<B> tempEager() { return this; }
    @Override public String       toString()  { return "EMPTY"; }
}
