package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindReleasedException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

public class BatchEmitter<B extends Batch<B>> extends SelfEmitter<B> {
    private @Nullable B batch;
    private int nextRow;
    private @Nullable B recycled;

    public BatchEmitter(Vars vars, B batch) {
        super(batch.type(), vars, EMITTER_SVC, RR_WORKER, CREATED, SELF_EMITTER_FLAGS);
        this.batch = batch;
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
    }

    @Override protected int produceAndDeliver(int state) {
        long limit = (long)REQUESTED.getOpaque(this);
        if (limit <= 0)
            return state;
        B batch = this.batch;
        if (batch == null)
            return COMPLETED;
        int n = (int)Math.min(limit, batch.rows-nextRow);
        REQUESTED.getAndAddRelease(this, (long)-n);
        B copy = batchType.empty(Batch.asUnpooled(recycled), n, batch.cols, batch.localBytesUsed());
        recycled = null;
        copy.putRange(batch, nextRow, nextRow+n);
        nextRow += n;
        recycled = Batch.asPooled(deliver(copy));
        return nextRow < batch.rows ? state : COMPLETED;
    }
}
