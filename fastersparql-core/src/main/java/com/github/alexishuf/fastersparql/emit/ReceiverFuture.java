package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public abstract class ReceiverFuture<T, B extends Batch<B>> extends CompletableFuture<T> implements Receiver<B> {
    private @MonotonicNonNull Emitter<B> emitter;

    public ReceiverFuture() { }

    public ReceiverFuture(Emitter<B> emitter) throws Emitter.RegisterAfterStartException {
        this.emitter = emitter;
        emitter.subscribe(this);
    }

    /**
     * Calls {@code emitter.subscribe(this)} and performs bookkeeping.
     *
     * @param emitter the emitter that will feed this {@link Receiver}
     * @throws Emitter.RegisterAfterStartException see {@link Emitter#subscribe(Receiver)}
     */
    public void subscribeTo(Emitter<B> emitter) throws Emitter.RegisterAfterStartException {
        if (this.emitter != null) {
            if (this.emitter == emitter) return;
            throw new IllegalStateException("Already subscribed to "+emitter);
        }
        this.emitter = emitter;
        emitter.subscribe(this);
    }

    /**
     * Equivalent to {@link #get()}, but is uninterruptible and returns
     * {@link RuntimeExecutionException} instead of {@link ExecutionException}.
     *
     * @return see {@link #get()}
     * @throws RuntimeExecutionException if {@link #get()} throws {@link ExecutionException}
     */
    public T getUnchecked() throws RuntimeExecutionException {
        boolean interrupted = false;
        T value;
        while (true) {
            try {
                value = super.get();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException e) {
                throw new RuntimeExecutionException(e.getCause());
            }
        }
        if (interrupted)
            Thread.currentThread().interrupt();
        return value;
    }

    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        if (isDone())
            return false;
        emitter.cancel();
        return true;
    }

    @Override public void onCancelled() {
        completeExceptionally(new CancellationException());
    }

    @Override public void onError(Throwable cause) {
        completeExceptionally(cause);
    }
}
