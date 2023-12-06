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
     * Atomically sets the field of {@code holder} accessed via {@code handle} to {@code offer}
     * if its current value is less than {@code offer}.
     *
     * @param handle A {@link VarHandle} for a {@code long} field in {@code holder}
     * @param holder object instance that has the {@code long} field accessed via {@code handle}
     * @param offer a value that if larger than the current value, will be written to the
     *              {@code long} field in {@code holder}
     * @return the result of {@code offer-actual}, where {@code actual} has been updated to the
     *         current value of the field. If {@code <= 0}, it means the field was not changed,
     *         else a value {@code > 0} indicates that the field has been set to {@code offer}
     */
    public static boolean maxRelease(VarHandle handle, Object holder, long offer) {
        for (long ac, ex=(long)handle.getAcquire(holder); offer > ex; ex = ac) {
            if ((ac=(long)handle.compareAndExchangeRelease(holder, ex, offer)) == ex)
                return true;
        }
        return false;
    }

    /**
     * Equivalent to {@link #maxRelease(VarHandle, Object, long)} but uses
     * {@link VarHandle#compareAndExchangeAcquire(Object...)}.
     */
    public static boolean maxAcquire(VarHandle handle, Object holder, long offer) {
        for (long ac, ex=(long)handle.getAcquire(holder); offer > ex; ex = ac) {
            if ((ac=(long)handle.compareAndExchangeAcquire(holder, ex, offer)) == ex)
                return true;
        }
        return false;
    }

    /**
     * Equivalent to {@link #maxRelease(VarHandle, Object, long)} but updates a
     * {@code int} field instead of a {@code long} field.
     */
    public static boolean maxRelease(VarHandle handle, Object holder, int offer) {
        for (int ac, ex=(int)handle.getAcquire(holder); offer > ex; ex = ac) {
            if ((ac=(int)handle.compareAndExchangeRelease(holder, ex, offer)) == ex)
                return true;
        }
        return false;
    }
}
