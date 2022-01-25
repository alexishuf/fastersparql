package com.github.alexishuf.fastersparql.client.util.async;

import com.github.alexishuf.fastersparql.client.util.Throwing;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class Async {
    private static @MonotonicNonNull ScheduledExecutorService SCHEDULED;
    private static @MonotonicNonNull ExecutorService UNBOUNDED;

    /* --- --- --- Future/ComletionState -> AsyncTask conversion --- --- --- */

    /**
     * Create a {@link SafeAsyncTask} already completed with {@code value}
     * @param value the result of the {@link AsyncTask}
     * @param <T> the type of {@code value}
     * @return an {@link SafeAsyncTask} completed with {@code value}
     */
    public static <T> SafeAsyncTask<T> wrap(T value) {
        SafeCompletableAsyncTask<T> task = new SafeCompletableAsyncTask<>();
        task.complete(value);
        return task;
    }

    /**
     * Get an {@link AsyncTask} that completes when and  as {@code future} completes.
     *
     * If {@code future} does not happen to implement the {@link CompletionStage} interface,
     * it will be polled, which may incur a delay between {@code future} transitioning to
     * the done state and the returned {@link AsyncTask} own transition.
     *
     * @param future The {@link Future} to wrap
     * @param <T> the result type of {@code future}
     * @return a new {@link AsyncTask} that will complete with {@code future} completion value
     *         or exception when it completes.
     */
    @SuppressWarnings("unchecked") public static <T> AsyncTask<T> wrap(Future<T> future) {
        if (future instanceof CompletionStage) {
            return wrap((CompletionStage<T>) future);
        }
        CompletableAsyncTask<T> task = new CompletableAsyncTask<>();
        poll(50, future::isDone).thenAccept(o -> {
            try {
                task.complete(future.get());
            } catch (ExecutionException e) {
                task.completeExceptionally(e.getCause() == null ? e : e.getCause());
            } catch (Throwable t) {
                task.completeExceptionally(t);
            }
        });
        return task;
    }

    /**
     * Create an {@link AsyncTask} that will be complete when and as {@code stage} completes.
     *
     * @param stage The {@link CompletionStage} to wrap
     * @param <T> the return type of {@code stage};
     * @return A new {@link AsyncTask} that will complete with stage result or exception.
     */
    public static <T> AsyncTask<T> wrap(CompletionStage<T> stage) {
        CompletableAsyncTask<T> task = new CompletableAsyncTask<>();
        stage.handle((r, t) -> t == null ? task.complete(r) : task.completeExceptionally(t));
        return task;
    }

    /**
     * Create an {@link AsyncTask} that completes with {@code null} when {@code poll} returns true.
     *
     * The result of the {@code poll} function is polled every {@code delayMs} milliseconds.
     *
     * @param delayMs how much time to wait before calling {@code poll} again.
     * @param poll a function that will return true once the desired state has been reached.
     * @return An {@link AsyncTask} that will complete with null once {@code poll} returns true.
     *         If the poll function throws, the {@link AsyncTask} will complete exceptionally
     *         with the thrown {@link Throwable}.
     */
    public static AsyncTask<?> poll(int delayMs, BooleanSupplier poll) {
        CompletableAsyncTask<Object> task = new CompletableAsyncTask<>();
        Runnable poller = new Runnable() {
            @Override public void run() {
                try {
                    boolean done = poll.getAsBoolean();
                    if (done) task.complete(null);
                    else      schedule(delayMs, TimeUnit.MILLISECONDS, this);
                } catch (Throwable t) {
                    task.completeExceptionally(t);
                }
            }
        };
        poller.run(); // test immediately
        return task;
    }


    /* --- --- --- dispatch methods --- --- --- */

    /**
     * Schedule the given {@code callable} for execution at least {@code delay} {@code unit}s
     * in the future.
     *
     * @param delay minimum delay until execution starts
     * @param unit {@link TimeUnit} of delay
     * @param callable what to execute. If null, the task will not be scheduled and the returned
     *                 {@link AsyncTask} will be done with a null result.
     * @param <T> type of result produced by the callable.
     * @return An {@link AsyncTask} that will expose the result (or failure) of the callable.
     */
    public static <T> AsyncTask<T> schedule(long delay, TimeUnit unit,
                                            @Nullable Callable<T> callable) {
        RunnableTask<T> task = new RunnableTask<>(callable);
        if (callable == null) return task;
        return task.cancelDelegate(scheduled().schedule(task, delay, unit));
    }

    /**
     * Equivalent to {@link Async#schedule(long, TimeUnit, Callable)} but with a runnable.
     *
     * @param delay minimum delay before execution of {@code runnable} starts.
     * @param unit {@link TimeUnit} of {@code delay}
     * @param runnable what to run. If null no scheduling occurs and the returned
     *                 {@link AsyncTask} will be already be complete iwth {@code null}.
     * @return An {@link AsyncTask} that will complete with null or with an {@link Throwable}
     *         thrown by {@code runnable}
     */
    public static AsyncTask<?> schedule(long delay, TimeUnit unit,
                                        @Nullable Runnable runnable) {
        RunnableTask<Object> task = new RunnableTask<>(runnable);
        if (runnable == null) return task;
        return task.cancelDelegate(scheduled().schedule(task, delay, unit));
    }

    /**
     * Equivalent to {@link Async#schedule(long, TimeUnit, Runnable)} but the runnable can
     * throw any {@link Exception}.
     */
    public static AsyncTask<?> scheduleThrowing(long delay, TimeUnit unit,
                                                Throwing.@Nullable Runnable runnable) {
        RunnableTask<Object> task = new RunnableTask<>(runnable);
        if (runnable == null) return task;
        return task.cancelDelegate(scheduled().schedule(task, delay, unit));
    }

    /**
     * Execute {@code callable.call()} on a pooled thread ASAP.
     *
     * The equivalent of a shared, long-lived {@link Executors#newCachedThreadPool()} is used. Thus,
     * execution will start soon, creating a new {@link Thread} if there is no free thread
     * in the pool.
     *
     * @param callable what to execute. If null, will not use the {@link ExecutorService},
     *                 instead returning an {@link AsyncTask} completed with a null value.
     * @param <T> the type of result produced by the {@code callable}
     * @return An {@link AsyncTask} representing the future result of {@code callable.call()}.
     */
    public static <T> AsyncTask<T> async(@Nullable Callable<T> callable) {
        RunnableTask<T> task = new RunnableTask<>(callable);
        return task.cancelDelegate(unbounded().submit(task));
    }

    /**
     * Execute {@code runnable} in a pooled thread ASAP.
     *
     * The equivalent of a shared, long-lived {@link Executors#newCachedThreadPool()} is used. Thus,
     * execution will start soon, creating a new {@link Thread} if there is no free thread
     * in the pool.
     *
     * @param runnable what to execute. If null will just return an already done
     *                 {@link AsyncTask} without using a thread.
     * @return An {@link AsyncTask} representing the future result ({@code null} or a
     *         {@link Throwable}) of {@code runnable.run()}.
     */
    public static AsyncTask<?> async(@Nullable Runnable runnable) {
        RunnableTask<Object> task = new RunnableTask<>(runnable);
        return task.cancelDelegate(unbounded().submit(task));
    }

    /**
     * Equivalent to {@link Async#async(Runnable)} but {@code runnable} can throw {@link Exception}s.
     */
    public static AsyncTask<?> asyncThrowing(Throwing.@Nullable Runnable runnable) {
        RunnableTask<Object> task = new RunnableTask<>(runnable);
        return task.cancelDelegate(unbounded().submit(task));
    }

    /* --- --- --- package-private helpers --- --- --- */

    /* --- --- --- implementation details --- --- --- */

    @RequiredArgsConstructor
    private static class Factory implements ThreadFactory {
        private final ThreadGroup group = System.getSecurityManager() == null
                ? Thread.currentThread().getThreadGroup()
                : System.getSecurityManager().getThreadGroup();
        private final AtomicInteger lastThreadId = new AtomicInteger(0);
        private final String factoryName;
        @Override public Thread newThread(@NonNull Runnable r) {
            String name = factoryName+"-"+lastThreadId.incrementAndGet();
            Thread thread = new Thread(group, r, name, 0);
            if (!thread.isDaemon())
                thread.setDaemon(true);
            if (thread.getPriority() != Thread.NORM_PRIORITY)
                thread.setPriority(Thread.NORM_PRIORITY);
            return thread;
        }
    }

    private static ScheduledExecutorService scheduled() {
        if (SCHEDULED == null) {
            Factory f = new Factory("FasterSparqlScheduled");
            int core = Math.min(1, Runtime.getRuntime().availableProcessors() / 2);
            SCHEDULED = new ScheduledThreadPoolExecutor(core, f);
        }
        return SCHEDULED;
    }

    private static ExecutorService unbounded() {
        if (UNBOUNDED == null)
            UNBOUNDED = Executors.newCachedThreadPool(new Factory("FasterSparql"));
        return UNBOUNDED;
    }

    @Accessors(fluent = true, chain = true)
    private static final class RunnableTask<T> extends CompletableAsyncTask<T>
            implements AsyncTask<T>, Runnable {
        private @Nullable Callable<T> callable;
        private @Nullable Runnable runnable;
        private Throwing.@Nullable Runnable throwingRunnable;
        private @Setter @MonotonicNonNull Future<?> cancelDelegate = null;

        public RunnableTask(@Nullable Callable<T> c) {
            if ((this.callable = c) == null) complete(null);
        }

        public RunnableTask(@Nullable Runnable r) {
            if ((this.runnable = r) == null) complete(null);
        }

        public RunnableTask(Throwing.@Nullable Runnable r) {
            if ((this.throwingRunnable = r) == null) complete(null);
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            return cancelDelegate != null && cancelDelegate.cancel(mayInterruptIfRunning);
        }

        @Override public boolean isCancelled() {
            return cancelDelegate != null && cancelDelegate.isCancelled();
        }

        @Override public void run() {
            try {
                if (callable != null) complete(callable.call());
                if (runnable != null) runnable.run();
                if (throwingRunnable != null) throwingRunnable.run();
                complete(null);
            } catch (Throwable t) {
                completeExceptionally(t);
            }
        }
    }

}
