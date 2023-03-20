package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;

import static java.lang.invoke.MethodHandles.lookup;

public final class ConverterBIt<B extends Batch<B>, S extends Batch<S>>
        extends DelegatedControlBIt<B, S> {
    private static final VarHandle RECYCLED;

    static {
        try {
            RECYCLED = lookup().findVarHandle(ConverterBIt.class, "recycled", Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final BatchType<B> batchType;
    private @Nullable S lastIn;
    @SuppressWarnings("unused") // accessed through RECYCLED
    private @Nullable B recycled;

    public ConverterBIt(BIt<S> delegate, BatchType<B> batchType) {
        super(delegate, batchType, delegate.vars());
        this.batchType = batchType;
    }

    @Override public @Nullable B nextBatch(@Nullable B out) {
        S in = delegate.nextBatch(lastIn);
        if (in == null) {
            batchType.recycle(stealRecycled());
            return null;
        }
        lastIn = in;
        if (out != null || (out = stealRecycled()) != null)
            out.clear(in.cols);
        else
            out = batchType.create(in.rows, in.cols, batchType.bytesRequired(in));
        return out.putConverting(in);
    }

    @Override public @Nullable B recycle(B batch) {
        return RECYCLED.compareAndExchange(this, null, batch) == null ? null : batch;
    }

    @Override public @Nullable B stealRecycled() {
        //noinspection unchecked
        return (B)RECYCLED.getAndSet(this, null);
    }
}
