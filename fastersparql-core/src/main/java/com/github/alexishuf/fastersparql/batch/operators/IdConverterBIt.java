package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class IdConverterBIt<B extends Batch<B>, S extends Batch<S>> extends ConverterBIt<B, S> {
    public IdConverterBIt(BIt<S> delegate, BatchType<B> batchType) {
        super(delegate, batchType);
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
        if (metrics != null) metrics.batch(out.totalRows());
        return out;
    }

    protected abstract B putConverting(B out, S in);
}
