package com.github.alexishuf.fastersparql.client.util.async;

import com.github.alexishuf.fastersparql.client.util.Throwing;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.util.concurrent.*;
import java.util.function.Consumer;

public interface AsyncTask<T> extends Future<T>, CompletionStage<T> {
    /**
     * Same as {@link Future#get()}, waits uninterruptibly.
     *
     * @return See {@link Future#get()}
     * @throws ExecutionException as in {@link Future#get()}
     */
    @Override T get() throws ExecutionException;

    /**
     * Same as {@link Future#get(long, TimeUnit)}, waits uninterruptibly.
     *
     * @return See {@link Future#get(long, TimeUnit)}
     * @throws ExecutionException as in {@link Future#get(long, TimeUnit)}
     */
    @Override
    T get(long timeout, @NonNull TimeUnit unit) throws ExecutionException, TimeoutException;

    /**
     * Uninterruptible {@link Future#get()} that throws an unchecked {@link ExecutionException}
     *
     * @return See {@link Future#get()}
     * @throws RuntimeExecutionException unchecked equivalent of {@link ExecutionException}
     */
    default T fetch() throws RuntimeExecutionException {
        try {
            return get();
        } catch (ExecutionException e) { throw new RuntimeExecutionException(e); }
    }

    /**
     * Uninterruptible {@link Future#get(long, TimeUnit)} that throws an unchecked
     * {@link ExecutionException}.
     *
     * @param timeout how much time to wait
     * @param unit unit of {@code timeout}
     * @return See {@link Future#get(long, TimeUnit)}
     * @throws RuntimeExecutionException unchecked equivalent of {@link ExecutionException}
     * @throws TimeoutException if the Future does not complete within the {@code timeout}.
     */
    default T fetch(long timeout, @NonNull TimeUnit unit) throws RuntimeExecutionException, TimeoutException {
        try {
            return get(timeout, unit);
        } catch (ExecutionException e) { throw new RuntimeExecutionException(e); }
    }

    /**
     * Wait and get the result (or exception) if the task is complete, else return {@code fallback}
     *
     * @param fallback what to return if the task has not been completed nor
     *                 {@link Future#cancel(boolean)}ed
     * @return The task result if it completed or else the {@code fallback}.
     * @throws ExecutionException if the task completed exceptionally.
     * @throws CancellationException if the task was previously cancelled.
     */
    default @PolyNull T orElse(T fallback) throws ExecutionException {
        return isDone() ? get() : fallback;
    }

    /**
     * {@link AsyncTask#orElse(Object)} with a timeout.
     *
     * @param fallback what to return if the task has not yet completed and does not complete
     *                 within the timeout.
     * @param timeout how much time to wait for completion.
     * @param unit {@link TimeUnit} of {@code timeout}
     * @return the task result, if completed, or {@code fallback} if not cancelled ana not
     *         completed within the given timeout
     * @throws ExecutionException If the task completed exceptionally
     * @throws CancellationException If the task was cancelled before or during the timeout wait.
     */
    default @PolyNull T orElse(T fallback, long timeout,
                               @NonNull TimeUnit unit) throws ExecutionException {
        try {
            return get(timeout, unit);
        } catch (TimeoutException e) {
            return fallback;
        }
    }

    /**
     * Get the task result, without waiting, or return the fallback value, throwing an unchecked
     * {@link ExecutionException} if the task completed exceptionally.
     *
     * @param fallback what to return if the task has not yet completed
     * @return the task result, if completed, else {@code fallback}
     * @throws RuntimeExecutionException if the task completed exceptionally
     * @throws CancellationException if the task was cancelled.
     */
    default @PolyNull T fetchOrElse(T fallback) throws RuntimeExecutionException {
        try {
            return orElse(fallback);
        } catch (ExecutionException e) { throw new RuntimeExecutionException(e); }
    }

    /**
     * Get the task result if it is complete or completes in within the {@code timeout},
     * else return the {@code fallback}.
     *
     * @param fallback  what to return if the task does not complete within the timeout.
     * @param timeout how much time to wait for task completion
     * @param unit {@link TimeUnit} of {!code timeout}
     * @return the task result if the task completes, else {@code fallback}
     * @throws RuntimeExecutionException if the task completed exceptionally within the allowed
     *                                   timeout.
     * @throws CancellationException if the task was cancelled before the timeout expires.
     */
    default @PolyNull T fetchOrElse(T fallback, long timeout,
                                    @NonNull TimeUnit unit) throws RuntimeExecutionException {
        try {
            return orElse(fallback, timeout, unit);
        } catch (ExecutionException e) { throw new RuntimeExecutionException(e); }
    }

    /**
     * Same as {@link CompletionStage#thenAccept(Consumer)} but with allows a throwing function.
     *
     * If {@code this} completes exceptionally, {@code function} will not be executed.
     *
     * @param function the function to apply to the succesful result of this {@link AsyncTask}
     * @param <U> The type returned by function.
     * @return A new {@link AsyncTask} that will complete with the result {@code function.apply()}
     *         applied to the result of {@code this} {@link AsyncTask}. If {@code this} completes
     *         exceptionally or if {@code function} throw, the returned {@link AsyncTask} will
     *         complete exceptionally with the {@link Throwable}.
     */
    default <U> AsyncTask<U> thenApplyThrowing(Throwing.Function<T, U> function) {
        CompletableAsyncTask<U> task = new CompletableAsyncTask<>();
        handle((result, cause) -> {
            if (cause == null) {
                try {
                    task.complete(function.apply(result));
                } catch (Throwable t) {
                    task.completeExceptionally(t);
                }
            } else {
                task.completeExceptionally(cause);
            }
            return null;
        });
        return task;
    }

    /**
     * Version of {@link CompletionStage#thenAccept(Consumer)} with a throwing {@link Consumer}.
     *
     * If {@code this} complestes exceptionally, {@code consumer} will not be executed and the
     * cause will be forwarded to the new {@link AsyncTask}
     *
     * @param consumer what to execute on the successful result of {@code this} {@link AsyncTask}.
     * @return A new {@link AsyncTask} that will complete with null if {@code consumer.accept()}
     *         completes without throwing. Else the {@link AsyncTask} will complete with the
     *         thrown {@link Throwable}.
     */
    default AsyncTask<?> thenAcceptThrowing(Throwing.Consumer<T> consumer) {
        CompletableAsyncTask<Object> task = new CompletableAsyncTask<>();
        handle((result, cause) -> {
            if (cause == null) {
                try {
                    consumer.accept(result);
                    task.complete(null);
                } catch (Throwable t) {
                    task.completeExceptionally(t);
                }
            } else {
                task.completeExceptionally(cause);
            }
            return null;
        });
        return task;
    }
}
