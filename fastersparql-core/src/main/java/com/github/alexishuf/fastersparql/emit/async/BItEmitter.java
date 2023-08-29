package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.exceptions.IllegalEmitStateException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Optional;
import java.util.stream.Stream;

public final class BItEmitter<B extends Batch<B>> extends SelfEmitter<B> {
    private BIt<B> it;
    private @Nullable B recycled;

    public BItEmitter(BIt<B> it) { this(EmitterService.EMITTER_SVC, RR_WORKER, it); }
    public BItEmitter(EmitterService runner, int worker, BIt<B> it) {
        super(it.batchType(), it.vars(), runner, worker, CREATED, SELF_EMITTER_FLAGS);
        this.it = it;
    }

    @Override protected void doRelease() {
        recycled = Batch.recyclePooled(recycled);
        super.doRelease();
    }

    public static final class StolenBItException extends Exception {
        private StolenBItException() {super("it stole with stealIt()");}
    }
    private static final StolenBItException STOLEN_ERROR = new StolenBItException();


    BIt<B> stealIt() {
        int state = state();
        if ((state&STATE_MASK) != CREATED)
            throw new IllegalEmitStateException("Cannot steal if not in CREATED state");
        BIt<B> it = this.it;
        this.it = null;
        this.error = STOLEN_ERROR;
        deliverTermination(state, FAILED);
        return it;
    }

    @Override public String toString() {
        return it == null ? "stolen BIt" : it.toString();
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Optional.ofNullable(it).stream();
    }

    @Override public void rebind(BatchBinding<B> binding) throws RebindException {
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
