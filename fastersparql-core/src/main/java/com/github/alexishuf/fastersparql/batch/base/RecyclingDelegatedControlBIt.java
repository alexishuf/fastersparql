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
    @SuppressWarnings("unused") private B plainRecycled;

    public RecyclingDelegatedControlBIt(BIt<S> delegate, BatchType<B> batchType, Vars vars) {
        super(delegate, batchType, vars);
    }

    @Override protected void cleanup(boolean cancelled, @Nullable Throwable error) {
        for (B b; (b = stealRecycled()) != null; )
            b.recycle();
    }

    @Override public @Nullable B recycle(B batch) {
        if (batch == null)
            return null;
        batch.markPooled();
        if (RECYCLED.compareAndExchange(this, null, batch) == null) return null;
        batch.unmarkPooled();
        return batch;
    }

    @Override public @Nullable B stealRecycled() {
        //noinspection unchecked
        return (B)RECYCLED.getAndSet(this, null);
    }
}
