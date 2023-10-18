package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.RecyclingDelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ConverterBIt<B extends Batch<B>, S extends Batch<S>>
        extends RecyclingDelegatedControlBIt<B, S> {
    protected final BatchType<B> batchType;
    protected @Nullable S lastIn;

    public ConverterBIt(BIt<S> delegate, BatchType<B> batchType) {
        super(delegate, batchType, delegate.vars());
        this.batchType = batchType;
    }

    @Override protected void cleanup(boolean cancelled, @Nullable Throwable error) {
        super.cleanup(cancelled, error);
        S in = lastIn;
        lastIn = null;
        if (in != null)
            in.recycle();
    }

    @Override public @Nullable B nextBatch(@Nullable B out) {
        S in = lastIn;
        lastIn = null;
        try {
            in = delegate.nextBatch(in);
        } catch (Throwable t) {
            onTermination(false, t);
            throw t;
        }
        if (in == null) {
            batchType.recycle(out);
            onTermination(false, null);
            return null;
        }
        if (out != null || (out = stealRecycled()) != null)
            out.clear(in.cols);
        else
            out = batchType.create(in.rows, in.cols);
        out = out.putConverting(in, null, null);
        lastIn = in;
        if (metrics != null) metrics.batch(out.rows);
        return out;
    }
}
