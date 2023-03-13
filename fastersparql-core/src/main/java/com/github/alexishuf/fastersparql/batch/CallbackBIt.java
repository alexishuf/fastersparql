package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.BItCompletedException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An asynchronous {@link BIt} that internally holds a bounded queue of batches.
 */
public interface CallbackBIt<T> extends BIt<T> {
    void feed(Batch<T> batch) throws BItCompletedException;
    void feed(T item) throws BItCompletedException;
    void complete(@Nullable Throwable error);
}
