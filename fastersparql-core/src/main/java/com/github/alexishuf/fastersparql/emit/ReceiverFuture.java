package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.OwnershipException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.*;
import com.github.alexishuf.fastersparql.util.owned.LeakDetector.LeakState;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.concurrent.*;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.GARBAGE;

public abstract class ReceiverFuture<T, B extends Batch<B>, R extends ReceiverFuture<T, B, R>>
        extends CompletableFuture<T>
        implements ExposedOwned<R>, Receiver<B> {
    private static final boolean TRACE        = OwnershipHistory.ENABLED;
    private static final boolean DETECT_LEAKS = LeakDetector.ENABLED;
    protected @Nullable Emitter<B, ?> upstream;
    protected @MonotonicNonNull CancellationException cancelledAt;
    private @Nullable Object owner;
    private final OwnershipHistory history;
    private final @Nullable LeakState leakState;
    private boolean started = false;

    public ReceiverFuture() {
        history = OwnershipHistory.createIfEnabled();
        if (LeakDetector.ENABLED)
            LeakDetector.register(this, leakState = new LeakState(this, history));
        else
            leakState = null;
    }

    /* --- --- --- Owned --- --- --- */

    @SuppressWarnings("unchecked") protected R takeOwnership0(Object newOwner) {
        unsafeUntracedExchangeOwner0(null, newOwner);
        if (TRACE && history != null)
            history.taken(this, newOwner);
        if (DETECT_LEAKS && leakState != null)
            leakState.update(newOwner);
        return (R)this;
    }

    @Override public @Nullable R recycle(Object currentOwner) {
        unsafeUntracedExchangeOwner0(currentOwner, GARBAGE);
        if (TRACE && history != null)
            history.recycled(this);
        if (DETECT_LEAKS && leakState != null)
            leakState.update(GARBAGE);
        assert upstream != null;
        Owned.recycle(upstream, this);
        return null;
    }

    @SuppressWarnings("unchecked") @Override
    public Orphan<R> releaseOwnership(Object currentOwner) {
        unsafeUntracedExchangeOwner0(currentOwner, null );
        if (TRACE && history != null)
            history.released(this);
        if (DETECT_LEAKS && leakState != null)
            leakState.update(null);
        return (Orphan<R>)this;
    }

    @SuppressWarnings("unchecked") @Override
    public @This R transferOwnership(Object currentOwner, Object newOwner) {
        unsafeUntracedExchangeOwner0(currentOwner, newOwner);
        if (TRACE && history != null)
            history.transfer(this, newOwner);
        if (DETECT_LEAKS && leakState != null)
            leakState.update(newOwner);
        return (R)this;
    }

    @Override public @Nullable Object unsafeInternalOwner0() {return owner;}
    @Override public @Nullable OwnershipHistory unsafeInternalLastOwnershipHistory() {
        return history;
    }
    @Override public void unsafeUntracedExchangeOwner0(@Nullable Object expected,
                                                       @Nullable Object newOwner) {
        if (owner != expected)
            throw new OwnershipException(this, expected, owner, history);
        owner = newOwner;
    }

    /**
     * Calls {@code emitter.subscribe(this)} and performs bookkeeping.
     *
     * @param emitter the emitter that will feed this {@link Receiver}
     * @return {@code this}
     * @throws RegisterAfterStartException see {@link Emitter#subscribe(Receiver)}
     */
    @SuppressWarnings("unchecked") public @This R
    subscribeTo(Orphan<? extends Emitter<B, ?>> emitter) throws RegisterAfterStartException {
        if (this.upstream != emitter) {
            if (this.upstream != null)
                throw new IllegalStateException("Already subscribed");
            this.upstream = emitter.takeOwnership(this);
            this.upstream.subscribe(this);
        }
        return (R)this;
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Stream.ofNullable(upstream);
    }

    public void start() {
        if (started) return;
        started = true;
        assert upstream != null;
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
        if (upstream != null)
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
