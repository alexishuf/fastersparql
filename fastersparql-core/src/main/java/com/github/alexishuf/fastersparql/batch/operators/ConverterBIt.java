package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
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
            in.recycle(this);
    }

    @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
        S in = lastIn;
        lastIn = null;
        try {
            in = delegate.nextBatch(in, this);
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
        if (in == null) {
            Orphan.recycle(offer);
            onTermination(null);
            return null;
        }
        if (!isTerminated()) {
            B out = offer == null ? batchType.create(in.cols).takeOwnership(this)
                                  : offer.takeOwnership(this).clear(in.cols);
            lock();
            try {
                if (isTerminated()) {
                    out.recycle(this);
                } else {
                    onNextBatch(offer = putConverting(out, in).releaseOwnership(this));
                    lastIn = in;
                    return offer;
                }
            } finally { unlock(); }
        }
        in.recycle(this);
        return null;
    }

    protected B putConverting(B out, S in) {
        out.putConverting(in);
        return out;
    }
}
