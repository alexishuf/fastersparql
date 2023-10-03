package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.concurrent.*;
import java.util.stream.Stream;

public abstract class ReceiverFuture<T, B extends Batch<B>> extends CompletableFuture<T> implements Receiver<B> {
    protected @MonotonicNonNull Emitter<B> upstream;
    protected @MonotonicNonNull CancellationException cancelledAt;
    private boolean started = false;

    @Override public String toString() {
        return String.format("%s@%x", getClass().getSimpleName(), System.identityHashCode(this));
    }

    /**
     * Calls {@code emitter.subscribe(this)} and performs bookkeeping.
     *
     * @param emitter the emitter that will feed this {@link Receiver}
     * @return {@code this}
     * @throws RegisterAfterStartException see {@link Emitter#subscribe(Receiver)}
     */
    public @This ReceiverFuture<T, B>
    subscribeTo(Emitter<B> emitter) throws RegisterAfterStartException {
        if (this.upstream == emitter) return this;
        if (this.upstream !=    null) throw new IllegalStateException("Already subscribed");
        this.upstream = emitter;
        emitter.subscribe(this);
        return this;
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.ofNullable(upstream);
    }

    public void start() {
        if (started) return;
        started = true;
        upstream.request(Long.MAX_VALUE);
    }

    /**
     * Equivalent to {@link #get()}, but is uninterruptible and returns
     * {@link RuntimeExecutionException} instead of {@link ExecutionException}.
     *
     * @return see {@link #get()}
     * @throws RuntimeExecutionException if {@link #get()} throws {@link ExecutionException}
     */
    public T getSimple() throws RuntimeExecutionException {
        start();
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return super.get();
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    throw new RuntimeExecutionException(e.getCause());
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Same as {@link #get(long, TimeUnit)}, but is uninterruptible and throws
     * {@link RuntimeExecutionException}
     * @param timeout see {@link #get(long, TimeUnit)}
     * @param timeUnit see {@link #get(long, TimeUnit)}
     * @return the result of this {@link CompletableFuture}, if completed within the given timeout
     * @throws TimeoutException If the timeout elapsed and the emitter did not terminate
     * @throws RuntimeExecutionException If the emitter terminated with an error
     */
    public T getSimple(long timeout, TimeUnit timeUnit)
            throws TimeoutException, RuntimeExecutionException {
        start();
        boolean interrupted = false;
        long parked, nanos = timeUnit.toNanos(timeout);
        try {
            while (nanos > 0) {
                parked = Timestamp.nanoTime();
                try {
                    return get(nanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    nanos -= Timestamp.nanoTime()-parked;
                } catch (ExecutionException e) {
                    throw new RuntimeExecutionException(e.getCause());
                }
            }
            throw new TimeoutException();
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    @Override public T get() throws InterruptedException, ExecutionException {
        start();
        return super.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        start();
        return super.get(timeout, unit);
    }

    @Override public T join() {
        start();
        return super.join();
    }

    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone())
            return false;
        cancelledAt = new CancellationException();
        upstream.cancel();
        return true;
    }

    @Override public void onCancelled() {
        completeExceptionally(cancelledAt == null ? new FSCancelledException() : cancelledAt);
    }

    @Override public void onError(Throwable cause) {
        completeExceptionally(cause);
    }
}
