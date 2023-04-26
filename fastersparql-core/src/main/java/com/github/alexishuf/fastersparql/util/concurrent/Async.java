package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;

import java.util.concurrent.BlockingQueue;
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

    public static void uninterruptibleJoin(Thread thread) {
        boolean interrupted = false;
        while (true) {
            try {
                thread.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    public static <T> void uninterruptiblePut(BlockingQueue<T> queue, T obj) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    queue.put(obj);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    public static <T> T uninterruptibleTake(BlockingQueue<T> queue) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return queue.take();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

}
