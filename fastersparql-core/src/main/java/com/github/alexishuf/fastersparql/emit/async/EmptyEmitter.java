package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public class EmptyEmitter<B extends Batch<B>> extends TaskEmitter<B> {
    public EmptyEmitter(BatchType<B> batchType, Vars vars, @Nullable Throwable error) {
        super(batchType, vars, EMITTER_SVC, RR_WORKER, CREATED, TASK_EMITTER_FLAGS);
        this.error = error == null ? UNSET_ERROR : error;
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        if (ResultJournal.ENABLED)
            ResultJournal.rebindEmitter(this, binding);
        resetForRebind(0, 0);
    }

    @Override protected int produceAndDeliver(int state) {
        return this.error == UNSET_ERROR ? COMPLETED : FAILED;
    }
}
