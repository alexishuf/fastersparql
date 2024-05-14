package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public abstract sealed class EmptyEmitter<B extends Batch<B>> extends TaskEmitter<B, EmptyEmitter<B>> {
    public static <B extends Batch<B>> Orphan<EmptyEmitter<B>>
    create(BatchType<B> batchType, Vars vars, @Nullable Throwable error) {
        return new Concrete<>(batchType, vars, error);
    }

    protected EmptyEmitter(BatchType<B> batchType, Vars vars, @Nullable Throwable error) {
        super(batchType, vars, EMITTER_SVC, CREATED, TASK_FLAGS);
        this.error = error == null ? UNSET_ERROR : error;
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    private static final class Concrete<B extends Batch<B>> extends EmptyEmitter<B>
            implements Orphan<EmptyEmitter<B>> {
        public Concrete(BatchType<B> batchType, Vars vars, @Nullable Throwable error) {
            super(batchType, vars, error);
        }
        @Override public EmptyEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        if (ResultJournal.ENABLED)
            ResultJournal.rebindEmitter(this, binding);
        resetForRebind(0, 0);
    }

    @Override public Vars bindableVars() { return Vars.EMPTY; }

    @Override protected int produceAndDeliver(int state) {
        return this.error == UNSET_ERROR ? COMPLETED : FAILED;
    }
}
