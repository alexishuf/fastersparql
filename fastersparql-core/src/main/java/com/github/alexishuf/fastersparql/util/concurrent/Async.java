package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

public class Async {
    private static final class StageSync<T> implements BiConsumer<T, Throwable> {
        T result;
        Throwable error;
        volatile boolean completed;
        Thread waiter = Thread.currentThread();

        @Override public void accept(T value, Throwable cause) {
            result = value;
            error = cause;
            completed = true;
            LockSupport.unpark(waiter);
        }
    }

    public static <T> T waitStage(CompletionStage<T> stage) {
        var sync = new StageSync<T>();
        stage.whenComplete(sync);
        while (!sync.completed) LockSupport.park();
        if (sync.error != null)
            throw new RuntimeExecutionException(sync.error);
        return sync.result;
    }

    public static <T> void completeWhenWith(CompletableFuture<? super T> completable,
                                            CompletionStage<?> stage, T value) {
        stage.whenComplete((ignored, err) -> {
            if (err == null) completable.complete(value);
            else             completable.completeExceptionally(err);
        });
    }
}
