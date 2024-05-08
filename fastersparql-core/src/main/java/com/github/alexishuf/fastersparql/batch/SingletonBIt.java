package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

public class SingletonBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private @Nullable B batch;

    public SingletonBIt(@Nullable Orphan<B> batch, BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
        this.batch = Orphan.takeOwnership(batch, this);
    }

    @Override public Orphan<B> nextBatch(@Nullable Orphan<B> b) {
         Orphan.recycle(b);
        lock();
        try {
            b = Owned.releaseOwnership(batch, this);
            batch = null;
            if (b == null && state() == State.ACTIVE)
                onTermination(null);
            else if (b != null)
                onNextBatch(b);
            return b;
        } finally { unlock(); }
    }

    @Override public @This BIt<B> tempEager() { return this; }
    @Override public      String  toString()  { return batch == null ? "EMPTY" : batch.toString(); }
}
