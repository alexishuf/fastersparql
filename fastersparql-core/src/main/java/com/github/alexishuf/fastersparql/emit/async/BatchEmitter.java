package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindReleasedException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

public class BatchEmitter<B extends Batch<B>> extends TaskEmitter<B> {
    private @Nullable B batch;
    private int nextRow;
    private @Nullable B recycled;

    public BatchEmitter(Vars vars, B batch) {
        super(batch.type(), vars, EMITTER_SVC, RR_WORKER, CREATED, TASK_EMITTER_FLAGS);
        this.batch = batch;
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    @Override protected void doRelease() {
        recycled = Batch.recyclePooled(recycled);
        batch = batchType.recycle(batch);
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

    @Override protected int produceAndDeliver(int state) {
        long limit = (long)REQUESTED.getOpaque(this);
        if (limit <= 0)
            return state;
        B b = this.batch;
        if (b == null)
            return COMPLETED;
        int n = (int)Math.min(limit, b.rows-nextRow);
        REQUESTED.getAndAddRelease(this, (long)-n);
        B copy = batchType.empty(Batch.asUnpooled(recycled), n, b.cols, b.localBytesUsed());
        recycled = null;
        if (nextRow == 0 && n == b.rows) {
            copy = copy.put(b);
        } else {
            for (int r = nextRow, end = nextRow+n; r < end; r++)
                copy.putRow(b, r);
        }
        nextRow += n;
        recycled = Batch.asPooled(deliver(copy));
        return nextRow < b.rows ? state : COMPLETED;
    }
}
