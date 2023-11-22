package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;

import java.lang.invoke.VarHandle;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

public class Async {
    private static final class StageSync<T> implements BiConsumer<T, Throwable> {
        T result;
        Throwable error;
        volatile boolean completed;
        final Thread waiter = Thread.currentThread();

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

    public static <T> CompletionStage<T> async(Callable<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                future.complete(task.call());
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    public static  CompletionStage<Void> async(ThrowingRunnable task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                task.run();
                future.complete(null);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
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

    public static void uninterruptibleSleep(int ms) {
        long start = Timestamp.nanoTime();
        boolean interrupted = false;
        while ((Timestamp.nanoTime()-start)/1_000_000L < ms) {
            try { //noinspection BusyWait
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Atomically performs {@code field = max(0, field) + add}, resulting in {@link Long#MAX_VALUE}
     * instead wrap-around in case of overflow.
     *
     * @param handle A {@link VarHandle} for a {@code long} field in {@code holder}
     * @param holder object instance that has the {@code long} field accessed via {@code handle}
     * @param curr The estimated current value of the {@code long} field in {@code holder}. If
     *             stale, a new value will be read before the operation is retried.
     * @param add value to atomically aff to the field in {@code holder}
     * @return the updated value
     */
    public static long safeAddAndGetRelease(VarHandle handle, Object holder, long curr, long add) {
        long next, ex;
        do {
            next = Math.max(0, ex=curr)+add;
            if (next < 0)
                next = Long.MAX_VALUE; // overflow
        } while ((curr=(long)handle.compareAndExchangeRelease(holder, ex, next)) != ex);
        return next;
    }
}
