package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Optional;
import java.util.stream.Stream;

public final class BItEmitter<B extends Batch<B>> extends TaskEmitter<B> {
    private final BIt<B> it;
    private @Nullable B recycled;

    public BItEmitter(BIt<B> it) { this(EmitterService.EMITTER_SVC, RR_WORKER, it); }
    public BItEmitter(EmitterService runner, int worker, BIt<B> it) {
        super(it.batchType(), it.vars(), runner, worker, CREATED, TASK_EMITTER_FLAGS);
        this.it = it;
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    @Override protected void doRelease() {
        recycled = Batch.recyclePooled(recycled);
        super.doRelease();
    }

    @Override public String toString() { return it.toString(); }

    @Override public Stream<? extends StreamNode> upstream() {
        return Optional.ofNullable(it).stream();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        throw new UnsupportedOperationException("Cannot rebind a BIt");
    }

    /* --- --- --- Task methods --- --- --- */

    @Override protected int produceAndDeliver(int state) {
        long limit = (long)REQUESTED.getOpaque(this);
        if (limit <= 0)
            return state;
        int termState = state;
        B b = Batch.asUnpooled(recycled);
        recycled = null;
        try {
            b = it.nextBatch(b);
            if (b == null)
                termState = COMPLETED;
        } catch (Throwable t) {
            error = t;
            termState = FAILED;
            b = null;
        }
        if (b != null) {
            if ((long) REQUESTED.getAndAddRelease(this, -b.rows) > b.rows)
                termState |= MUST_AWAKE;
            recycled = Batch.asPooled(deliver(b));
        }
        return termState;
    }
}