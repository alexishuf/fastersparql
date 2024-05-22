package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
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
        if (isTerminated())
            return null;
        B out = null;
        S in = null;
        lock();
        boolean locked = true;
        try {
            Orphan<S> inOffer = Owned.releaseOwnership(lastIn, this);
            lastIn = null;
            unlock();
            locked = false;
            in = Orphan.takeOwnership(delegate.nextBatch(inOffer), this);
            if (in == null) {
                onTermination(null);
            } else {
                out = offer == null ? batchType.create(in.cols).takeOwnership(this)
                                    : offer.takeOwnership(this).clear(in.cols);
                offer = null;
                lock();
                try {
                    if (!isTerminated()) { // re-check after lock() for concurrent tryCancel()
                        var ret = putConverting(out, in).releaseOwnership(this);
                        lastIn  = in;
                        out     = null;
                        in      = null;
                        onNextBatch(ret);
                        return ret;
                    }
                } finally { unlock(); }
            }
        } catch (Throwable t){
            onTermination(t);
            throw t;
        } finally {
            if (locked) unlock();
            if (offer != null) Orphan.safeRecycle(offer);
            if (out   != null) Batch .safeRecycle(out, this);
            if (in    != null) Batch .safeRecycle(in,  this);
        }
        return null;
    }

    protected B putConverting(B out, S in) {
        out.putConverting(in);
        return out;
    }
}
