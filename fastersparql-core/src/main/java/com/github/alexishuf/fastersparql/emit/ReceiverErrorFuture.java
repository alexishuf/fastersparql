package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;

public abstract class ReceiverErrorFuture<B extends Batch<B>> extends ReceiverFuture<Throwable, B> {
    @Override public void onComplete()             { complete(null); }
    @Override public void onError(Throwable cause) { complete(cause); }
    @Override public void onCancelled() {
        complete(cancelledAt == null ? new FSCancelledException() : cancelledAt);
    }
}
