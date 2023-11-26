package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;

import java.util.Optional;
import java.util.stream.Stream;

public final class BItEmitter<B extends Batch<B>> extends TaskEmitter<B> {
    private final BIt<B> it;

    public BItEmitter(BIt<B> it) { this(EmitterService.EMITTER_SVC, RR_WORKER, it); }
    public BItEmitter(EmitterService runner, int worker, BIt<B> it) {
        super(it.batchType(), it.vars(), runner, worker, CREATED, TASK_EMITTER_FLAGS);
        this.it = it;
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    @Override protected void doRelease() {
        super.doRelease();
        it.close();
    }

    @Override public String toString() { return it.toString(); }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Optional.ofNullable(it).stream();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        throw new UnsupportedOperationException("Cannot rebind a BIt");
    }

     @Override public Vars bindableVars() { return Vars.EMPTY; }

    @Override public void cancel() {
        super.cancel();
        it.close();
    }

    /* --- --- --- Task methods --- --- --- */

    @Override protected int produceAndDeliver(int state) {
        long limit = (long)REQUESTED.getOpaque(this);
        if (limit <= 0)
            return state;
        int termState = state;
        B b;
        try {
            b = it.nextBatch(null);
            if (b == null)
                termState = COMPLETED;
        } catch (Throwable t) {
            b = null;
            int st = state();
            if      ((st&IS_CANCEL_REQ) != 0) termState = CANCELLED;
            else if ((st&IS_TERM) != 0)       termState = st;
            else                              throw t;
        }
        if (b != null) {
            int bRows = b.totalRows();
            if ((long) REQUESTED.getAndAddRelease(this, -bRows) > bRows)
                termState |= MUST_AWAKE;
            bt.recycleForThread(threadId, deliver(b));
        }
        return termState;
    }
}
