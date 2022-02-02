package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A single-subscriber {@link Publisher} that can be safely fed from a callback-based producer
 *
 * Callback-based producers should notify an adapter that calls the
 * {@link CallbackPublisher#feed(Object)} method one or more times and eventually calls the
 * {@link CallbackPublisher#complete(Throwable)} method. The
 * {@link CallbackPublisher#complete(Throwable)} method is idempotent, so only the first call
 * will have an effect.
 *
 * {@link CallbackPublisher#feed(Object)} and {@link CallbackPublisher#complete(Throwable)} can
 * be called even before a {@link CallbackPublisher#subscribe(Subscriber)}. Early notifications
 * will be held in an unbounded queue until the subscriber arrives. After subscription,
 * backpressure can be passed to the original producer either via the
 * {@link CallbackPublisher#onRequest(long)} or {@link CallbackPublisher#onBackpressure} method.
 * The former is called for every positive {@link Subscription#request(long)} and the latter
 * when the original producer {@link CallbackPublisher#feed(Object)}s an item beyond the currently
 * requested amount (which is always the case if there is no subscriber yet).
 *
 * @param <T>
 */
public abstract class CallbackPublisher<T> implements Publisher<T> {
    private static final Logger log = LoggerFactory.getLogger(CallbackPublisher.class);

    private @MonotonicNonNull Subscriber<? super T> subscriber;
    private long requested = 0;
    private final AtomicInteger flushing = new AtomicInteger();
    private final AtomicBoolean terminated = new AtomicBoolean();
    private final Queue<Object> queue = new ConcurrentLinkedDeque<>();

    /* --- --- --- methods called by the callback adapter --- --- --- */

    /**
     * Delivers the given item to {@link Subscriber#onNext(Object)}.
     *
     * If the subscriber has not yet attached, the item will be queued. If the subscriber has
     * cancelled or {@link CallbackPublisher#complete(Throwable)} has already been called,
     * then the {@code item} will be discarded.
     *
     * @param item the item to deliver to {@link Subscriber#onNext(Object)}.
     */
    public void feed(T item) {
        queue.add(item);
        synchronized (this) {
            if (requested == 0) {
                requested = -1;
                onBackpressure();
            }
        }
        flush();
    }

    /**
     * Completes the {@link Publisher} (and thus the {@link Subscriber}), optionally with an error.
     *
     * @param error If null, {@link Subscriber#onComplete()} will be called, else
     *              {@link Subscriber#onError(Throwable)} will be called with this {@code error}
     */
    public void complete(@Nullable Throwable error) {
        log.trace("{}.complete({})", this, error);
        queue.add(error == null ? new CompleteMessage() : new ErrorMessage(error));
        flush();
    }

    /* --- --- --- Event methods to be implemented --- --- --- */

    /**
     * This method will be called for every {@link Subscription#request(long)} with {@code n > 0}.
     *
     * @param n the number of additional items requested.
     */
    protected abstract void onRequest(long n);

    /**
     * This method will be called for every item produced beyond the ammount previously requested
     * by the {@link Subscriber}. If there is no subscriber yet, this will be called for every
     * {@link CallbackPublisher#feed(Object)}.
     *
     * An implementation should instruct the original callback-based publisher to pause production.
     * Production should resume quickly once {@link CallbackPublisher#onRequest(long)} is called.
     */
    protected abstract void onBackpressure();

    /**
     * This method will be called on the first call of {@link Subscription#cancel()}.
     *
     * Subsequent calls to {@link Subscription#cancel()} are no-ops, thus this method will not
     * be called.
     */
    protected abstract void onCancel();

    /* --- --- --- Publisher methods --- --- --- */

    @Override public void subscribe(Subscriber<? super T> s) {
        if (subscriber != null) {
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) { }
                @Override public void cancel() { }
            });
            s.onError(new IllegalStateException(this+" only accepts a single subscriber"));
        } else {
            subscriber = s;
            s.onSubscribe(new Subscription() {
                @Override public void request(long n) {
                    log.trace("request({})", n);
                    if (n < 0) {
                        complete(new IllegalArgumentException("request("+n+"), expected > 0"));
                    } else if (!terminated.get()) {
                        synchronized (CallbackPublisher.this) {
                            requested = Math.max(0, requested) + n;
                        }
                        onRequest(n);
                        flush();
                    }
                }
                @Override public void cancel() {
                    terminate("cancel()", null);
                    onCancel();
                }
            });
            flush();
        }
    }

    /* --- --- --- implementation details --- --- --- */

    private boolean terminate(String reason, @Nullable Throwable cause) {
        if (!terminated.compareAndSet(false, true))
            return false;
        if (log.isTraceEnabled()) {
            if (cause != null)
                log.trace("{}.terminate(). Reason: {}", this, reason, cause);
            else
                log.trace("{}.terminate(). Reason: {}", this, reason);
            Object o;
            while ((o = queue.poll()) != null)
                log.trace("terminate(): Dropping queued {}", o);
        } else {
            queue.clear();
        }
        synchronized (this) {
            requested = Math.min(requested, 0);
        }
        return true;
    }

    private synchronized @Nullable Object dequeueRequested() {
        if (subscriber == null)
            return null;
        Object o = null;
        if (requested > 0) {
            if ((o = queue.poll()) != null)
                --requested;
        } else if (queue.peek() instanceof TerminateMessage) {
            o = queue.poll();
        }
        return o;
    }

    private void flush() {
        if (flushing.getAndIncrement() != 0)
            return;  //other thread (or an ancestor frame in this thread) is flushing
        try {
            do {
                for (Object obj = dequeueRequested(); obj != null; obj = dequeueRequested()) {
                    if (obj instanceof TerminateMessage) {
                        ((TerminateMessage) obj).execute();
                    } else {
                        try {
                            //noinspection unchecked
                            subscriber.onNext((T) obj);
                        } catch (Throwable t) {
                            terminate("onNext() failed, treating as cancel()ed", t);
                        }
                    }
                }
            } while (flushing.getAndDecrement() > 1);
        } catch (Throwable t) {
            flushing.set(0); // do not die with the lock, causing starvation.
            throw t;
        }
    }

    private interface TerminateMessage {
        void execute();
    }

    private class ErrorMessage implements TerminateMessage {
        private final Throwable cause;
        private ErrorMessage(Throwable cause) { this.cause = cause; }
        @Override public void execute() {
            if (terminate("producer notified error", cause)) {
                try {
                    subscriber.onError(cause);
                } catch (Throwable t) {
                    log.warn("{}.onError({}) threw {}. Ignoring", subscriber, cause, t);
                }
            } else {
                log.debug("Dropping {} received after publisher terminated", cause, cause);
            }
        }
    }

    private class CompleteMessage implements TerminateMessage {
        @Override public void execute() {
            if (terminate("producer notified complete", null)) {
                try {
                    subscriber.onComplete();
                } catch (Throwable t) {
                    log.warn("{}.onComplete() threw {}. Ignoring", subscriber, t);
                }
            } else {
                log.debug("Ignoring complete message received after publisher terminated");
            }
        }
    }

}
