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

    @Override public @Nullable B nextBatch(@Nullable B out) {
        S in = delegate.nextBatch(lastIn);
        if (in == null) {
            batchType.recycle(out);
            batchType.recycle(stealRecycled());
            return null;
        }
        lastIn = in;
        if (out != null || (out = stealRecycled()) != null)
            out.clear(in.cols);
        else
            out = batchType.create(in.rows, in.cols, batchType.bytesRequired(in));
        out = out.putConverting(in);
        if (metrics != null) metrics.batch(out.rows);
        return out;
    }
}
