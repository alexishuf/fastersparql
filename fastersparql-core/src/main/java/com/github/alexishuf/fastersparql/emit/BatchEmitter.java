package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindReleasedException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

public abstract sealed class BatchEmitter<B extends Batch<B>> extends TaskEmitter<B, BatchEmitter<B>> {
    private @Nullable B batch;
    private int nextRow;

    public static <B extends Batch<B>> Orphan<BatchEmitter<B>> create(Vars vars, Orphan<B> batch) {
        return new Concrete<>(vars, batch);
    }

    protected BatchEmitter(Vars vars, Orphan<B> batch) {
        super(Batch.peekType(batch), vars, EMITTER_SVC, RR_WORKER, CREATED, TASK_FLAGS);
        this.batch = batch.takeOwnership(this);
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    private static final class Concrete<B extends Batch<B>> extends BatchEmitter<B>
            implements Orphan<BatchEmitter<B>> {
        public Concrete(Vars vars, Orphan<B> batch) {super(vars, batch);}
        @Override public BatchEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override protected void doRelease() {
        batch = Batch.safeRecycle(batch, this);
        super.doRelease();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (batch == null)
            throw new RebindReleasedException(this);
        nextRow = 0;
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        if (ResultJournal.ENABLED)
            ResultJournal.rebindEmitter(this, binding);
    }

    @Override public Vars bindableVars() { return Vars.EMPTY; }

    @Override protected int produceAndDeliver(int state) {
        long limit = requested();
        if (limit <= 0)
            return state;
        B b = this.batch;
        int bRows = b == null ? 0 : b.totalRows();
        if (bRows == 0)
            return COMPLETED;

        int first = nextRow;
        int n = (int)Math.min(limit, bRows-nextRow);
        nextRow += n;
        if (first == 0 && n == bRows) {
            deliverByCopy(b);
        } else {
            B copy = bt.createForThread(threadId, b.cols).takeOwnership(this);
            for (int r = first, end = first+n; r < end; r++)
                copy.putRow(b, r);
            deliver(copy.releaseOwnership(this));
        }
        return nextRow < bRows ? state : COMPLETED;
    }
}
