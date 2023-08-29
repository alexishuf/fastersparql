package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public class EmptyEmitter<B extends Batch<B>> extends SelfEmitter<B> {
    public EmptyEmitter(BatchType<B> batchType, Vars vars, @Nullable Throwable error) {
        super(batchType, vars, EMITTER_SVC, RR_WORKER, CREATED, SELF_EMITTER_FLAGS);
        this.error = error == null ? UNSET_ERROR : error;
    }

    @Override public void rebind(BatchBinding<B> binding) throws RebindException {
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        resetForRebind(0, 0);
    }

    @Override protected int produceAndDeliver(int state) {
        return this.error == UNSET_ERROR ? COMPLETED : FAILED;
    }
}
