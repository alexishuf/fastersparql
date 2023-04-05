package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.ConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;

import static java.lang.invoke.MethodHandles.lookup;

public abstract class RecyclingDelegatedControlBIt<B extends Batch<B>, S extends Batch<S>>
        extends DelegatedControlBIt<B, S> {
    private static final VarHandle RECYCLED;
    static {
        try {
            RECYCLED = lookup().findVarHandle(ConverterBIt.class, "plainRecycled", Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private B plainRecycled;

    public RecyclingDelegatedControlBIt(BIt<S> delegate, BatchType<B> batchType, Vars vars) {
        super(delegate, batchType, vars);
    }

    @Override public @Nullable B recycle(B batch) {
        return RECYCLED.compareAndExchange(this, null, batch) == null ? null : batch;
    }

    @Override public @Nullable B stealRecycled() {
        //noinspection unchecked
        return (B)RECYCLED.getAndSet(this, null);
    }
}
