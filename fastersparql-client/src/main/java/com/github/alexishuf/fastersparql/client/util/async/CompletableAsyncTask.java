package com.github.alexishuf.fastersparql.client.util.async;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.*;

public class CompletableAsyncTask<T> extends CompletableFuture<T> implements AsyncTask<T> {
    /**
     * Makes this {@link AsyncTask} complete with the result or error of {@code stage},
     * when {@code stage} completes.
     *
     * This method will not block waiting for completion of {@code stage}
     *
     * @param stage the {@link CompletionStage} (or {@link AsyncTask}) to monitor
     * @return {@code this} {@link CompletableAsyncTask}.
     */
    public CompletableAsyncTask<T> completeWhen(CompletionStage<? extends T> stage) {
        stage.whenComplete((val, cause) -> {
            if (cause != null) completeExceptionally(cause);
            else complete(val);
        });
        return this;
    }

    @Override public T get() throws ExecutionException {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return super.get();
                } catch (InterruptedException e) { interrupted = true; }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }

    }

    @Override
    public T get(long timeout, @NonNull TimeUnit unit) throws ExecutionException, TimeoutException {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return super.get(timeout, unit);
                } catch (InterruptedException e) { interrupted = true; }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }

    }
}
