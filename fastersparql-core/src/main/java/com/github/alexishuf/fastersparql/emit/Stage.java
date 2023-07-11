package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public abstract class Stage<I extends Batch<I>, O extends Batch<O>>
        implements Emitter<O>, Receiver<I> {
    protected @MonotonicNonNull Emitter<I> upstream;
    protected Receiver<O> downstream;

    @Override public void subscribe(Receiver<O> receiver)
            throws RegisterAfterStartException, MultipleRegistrationUnsupported {
        if (downstream != null)
            throw new MultipleRegistrationUnsupported();
        downstream = receiver;
    }

    @Override public void cancel() {
        upstream.cancel();
    }

    @Override public void request(long rows) {
        upstream.request(rows);
    }

    @Override public void onComplete() {
        downstream.onComplete();
    }

    @Override public void onCancelled() {
        downstream.onCancelled();
    }

    @Override public void onError(Throwable cause) {
        downstream.onError(cause);
    }
}
