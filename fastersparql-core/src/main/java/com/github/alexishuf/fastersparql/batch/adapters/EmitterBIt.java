package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import org.checkerframework.checker.nullness.qual.Nullable;

public class EmitterBIt<B extends Batch<B>> extends SPSCBIt<B> implements Receiver<B>  {
    private final Emitter<B> upstream;
    private Throwable cancelCause = CancelledException.INSTANCE;

    public EmitterBIt(Emitter<B> emitter) {
        super(emitter.batchType(), emitter.vars());
        this.upstream = emitter;
        emitter.subscribe(this);
        emitter.request(maxItems);
    }


    @Override public @Nullable B onBatch(B batch) {
        try {
            int n = maxItems - batch.totalRows();
            if (n > 0) upstream.request(n);
            batch = offer(batch);
        } catch (Throwable t) {
            if (cancelCause == CancelledException.INSTANCE) cancelCause = t;
            upstream.cancel();
        }
        return batch;
    }

    @Override public void onComplete()             { if (!isTerminated()) complete(null); }
    @Override public void onError(Throwable cause) {                      complete(cause); }
    @Override public void onCancelled()            { if (!isTerminated()) complete(cancelCause); }
}
