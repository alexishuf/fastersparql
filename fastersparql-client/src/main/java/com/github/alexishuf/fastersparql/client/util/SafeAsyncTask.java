package com.github.alexishuf.fastersparql.client.util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * An {@link AsyncTask} that never completes exceptionally and is never cancelled.
 *
 * @param <T> the result type
 */
public interface SafeAsyncTask<T> extends AsyncTask<T> {

    @Override T get();

    @Override
    T get(long timeout, @NonNull TimeUnit unit) throws TimeoutException;

    @Override @PolyNull default T orElse(T fallback) {
        try {
            return AsyncTask.super.orElse(fallback);
        } catch (ExecutionException e) {
            throw new RuntimeException("Unexpected "+e, e);
        }
    }

    @Override @PolyNull
    default T orElse(T fallback, long timeout, @NonNull TimeUnit unit) {
        try {
            return AsyncTask.super.orElse(fallback, timeout, unit);
        } catch (ExecutionException e) {
            throw new RuntimeException("Unexpected "+e, e);
        }
    }
}
