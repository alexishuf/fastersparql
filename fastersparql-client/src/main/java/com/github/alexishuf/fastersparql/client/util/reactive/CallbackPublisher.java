package com.github.alexishuf.fastersparql.client.util.reactive;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final AtomicInteger nextAnonId = new AtomicInteger(1);

    private final String name;
    private final ReactiveEventQueue<T> queue;

    public CallbackPublisher(@Nullable String name) {
        this.name = name != null ? name : "CallbackPublisher-"+nextAnonId.getAndIncrement();
        queue = new ReactiveEventQueue<T>(this.name) {
            @Override protected void pause() {
                onBackpressure();
            }
            @Override protected void onRequest(long n) {
                CallbackPublisher.this.onRequest(n);
            }
            @Override protected void onTerminate(Throwable cause, boolean cancel) {
                if (cancel)
                    onCancel();
            }
        };
    }

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
        log.trace("{}.feed({})", this, item);
        queue.send(item).flush();
    }

    /**
     * Completes the {@link Publisher} (and thus the {@link Subscriber}), optionally with an error.
     *
     * @param error If null, {@link Subscriber#onComplete()} will be called, else
     *              {@link Subscriber#onError(Throwable)} will be called with this {@code error}
     */
    public void complete(@Nullable Throwable error) {
        log.trace("{}.complete({})", this, error);
        queue.sendComplete(error).flush();
    }

    @Override public String toString() {
        return name;
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
        queue.subscribe(s);
    }
}
