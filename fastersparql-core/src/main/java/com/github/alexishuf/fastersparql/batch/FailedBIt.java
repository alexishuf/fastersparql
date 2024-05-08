package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

public class FailedBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private final RuntimeException error;

    public FailedBIt(BatchType<B> batchType, Vars vars, Throwable error) {
        super(batchType, vars);
        this.error = error instanceof RuntimeException re ? re : new RuntimeException(error);
    }

    public FailedBIt(BatchType<B> bt, Throwable error) { this(bt, Vars.EMPTY, error); }

    @Override public Orphan<B> nextBatch(@Nullable Orphan<B> orphan) {
        lock();
        try {
            Orphan.recycle(orphan);
            if (state() == State.ACTIVE)
                onTermination(error);
            throw error;
        } finally { unlock(); }
    }

    @Override public @This BIt<B> tempEager() { return this; }
    @Override public String       toString()  { return "FAIL("+error+")"; }
}
