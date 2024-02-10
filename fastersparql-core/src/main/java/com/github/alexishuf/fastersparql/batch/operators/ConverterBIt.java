package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ConverterBIt<B extends Batch<B>, S extends Batch<S>>
        extends DelegatedControlBIt<B, S> {
    protected final BatchType<B> batchType;
    protected @Nullable S lastIn;

    public ConverterBIt(BIt<S> delegate, BatchType<B> batchType) {
        super(delegate, batchType, delegate.vars());
        this.batchType = batchType;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
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
            onTermination(t);
            throw t;
        }
        if (in == null) {
            batchType.recycle(out);
            onTermination(null);
            return null;
        }
        lock();
        try {
            if (isTerminated()) {
                delegate.batchType().recycle(in);
                return null;
            } else {
                out = putConverting(batchType.empty(out, in.cols), in);
                lastIn = in;
            }
        } finally { unlock(); }
        onNextBatch(out);
        return out;
    }

    protected B putConverting(B out, S in) {
        out.putConverting(in);
        return out;
    }
}
