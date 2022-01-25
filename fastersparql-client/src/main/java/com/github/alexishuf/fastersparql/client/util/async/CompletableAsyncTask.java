package com.github.alexishuf.fastersparql.client.util.async;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CompletableAsyncTask<T> extends CompletableFuture<T> implements AsyncTask<T> {
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
