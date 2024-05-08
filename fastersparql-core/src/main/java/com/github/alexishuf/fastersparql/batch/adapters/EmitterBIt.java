package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

public class EmitterBIt<B extends Batch<B>> extends SPSCBIt<B> implements Receiver<B>  {
    private final Emitter<B, ?> upstream;
    private Throwable cancelCause = CancelledException.INSTANCE;

    protected EmitterBIt(Orphan<? extends Emitter<B, ?>> orphan) {
        super(Emitter.peekBatchType(orphan), Emitter.peekVars(orphan));
        upstream = orphan.takeOwnership(this);
        upstream.subscribe(this);
        upstream.request(maxItems);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        upstream.recycle(this);
    }

    @Override public void onBatch(Orphan<B> orphan) {
        try {
            int n = maxItems - Batch.peekTotalRows(orphan);
            if (n > 0) upstream.request(n);
            offer(orphan);
        } catch (Throwable t) {
            if (cancelCause == CancelledException.INSTANCE) cancelCause = t;
            upstream.cancel();
        }
    }

    @Override public void onBatchByCopy(B batch) {
        try {
            int n = maxItems - batch.totalRows();
            if (n > 0) upstream.request(n);
            copy(batch);
        } catch (Throwable t) {
            if (cancelCause == CancelledException.INSTANCE) cancelCause = t;
            upstream.cancel();
        }
    }

    @Override public void onComplete()             { if (!isTerminated()) complete(null); }
    @Override public void onError(Throwable cause) {                      complete(cause); }
    @Override public void onCancelled()            { if (!isTerminated()) complete(cancelCause); }
}
