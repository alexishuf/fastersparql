package com.github.alexishuf.fastersparql.client.util.reactive;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class CallbackPublisher2<T> extends ReactiveEventLoopSketch.BoundPublisher<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final String name;
    private final ArrayDeque<T> overDelivered = new ArrayDeque<>();
    private final SubscriberAdapter subscriber = new SubscriberAdapter();
    private Throwable error = null;
    private TerminationState terminationState = TerminationState.NONE;
    private long requested = 0;

    /**
     * Creates a new publisher with a new {@link ReactiveEventLoopSketch} runnin on the global
     * {@link BoundedEventLoopPool#get()}.
     *
     * @param name the name for both the {@link CallbackPublisher2} and the
     *             {@link ReactiveEventLoopSketch}. If null, unique (and unrelated) names will be
     *             generated for both objects.
     */
    public CallbackPublisher2(@Nullable String name) {
        this(name != null ? name : "CallbackPublisher-"+nextId.getAndIncrement(),
             BoundedEventLoopPool.get());
    }

    /**
     * Creates a new {@link ReactiveEventLoopSketch} in pool and create a publisher running in that loop
     * @param name the name to be used for both the {@link CallbackPublisher2} and the
     *             {@link ReactiveEventLoopSketch}. If {@code null}, unrelated unique names will be
     *             generated for both objects.
     * @param pool the {@link BoundedEventLoopPool} where the created {@link ReactiveEventLoopSketch}
     *             will reside. If {@code null}, will use {@link BoundedEventLoopPool#get()}
     */
    public CallbackPublisher2(String name, @Nullable BoundedEventLoopPool pool) {
        this(name, new ReactiveEventLoopSketch(pool, name));
    }

    /**
     * Creates a {@link CallbackPublisher2} with given {@code name} running events on the
     * given {@code loop}.
     * @param name the name for the {@link CallbackPublisher2}. If null a unique name will be
     *             generated
     * @param loop the {@link ReactiveEventLoopSketch} where events will be processed. If null, wiil
     *             create a new {@link ReactiveEventLoopSketch} in {@link BoundedEventLoopPool#get()}
     *             using the given {@code name}
     */
    public CallbackPublisher2(String name, @Nullable ReactiveEventLoopSketch loop) {
        super(loop != null ? loop : new ReactiveEventLoopSketch(null, name),
              new PublisherAdapter<>());
        this.name = name != null ? name : "CallbackPublisher-"+nextId.getAndIncrement();
        ((PublisherAdapter<T>) delegate).outer = this;
    }

    public void feed(T item) {
        loop.next(subscriber, item);
    }

    public void complete(@Nullable Throwable error) {
        if (error == null)
            loop.complete(subscriber);
        else
            loop.error(subscriber, error);
    }

    protected abstract void onRequest(long n);
    protected abstract void onBackpressure();
    protected abstract void onCancel();

    @Override public String toString() {
        return name;
    }

    /* --- --- --- implementation details --- --- --- */

    private enum TerminationState {
        NONE,
        PENDING,
        DELIVERED
    }

    /**
     * Receives {@link Subscriber} events from the event loop and forwards them to a delegate.
     */
    private class SubscriberAdapter implements Subscriber<T> {
        private @MonotonicNonNull Subscriber<? super T> delegate = null;
        private boolean subscribed = false;
        private boolean checkActiveFailed = false;

        boolean offerDelegate(Subscriber<? super T> subscriber) {
            assert loop.inLoopThread() : "not in event loop thread";
            assert subscriber != null : "offering a null subscriber";
            boolean accept = delegate == null;
            if (accept)
                delegate = subscriber;
            return accept;
        }

        int flushOverDelivered() {
            assert loop.inLoopThread() : "not in event loop thread";
            if (!subscribed)
                return 0;
            int delivered = 0;
            while (requested > 0 && !overDelivered.isEmpty()) {
                --requested;
                ++delivered;
                delegate.onNext(overDelivered.remove());
            }
            if (delivered > 0) {
                log.trace("{} delivered {}{} items in overDelivered",
                          name, overDelivered.isEmpty() ? "all ": "", delivered);
            }
            if (overDelivered.isEmpty() && terminationState == TerminationState.PENDING) {
                terminationState = TerminationState.DELIVERED;
                if (error != null) {
                    log.trace("{}: delivering delayed onError({})", name, error);
                    delegate.onError(error);
                } else {
                    log.trace("{}: delivering delayed onComplete()", name);
                    delegate.onComplete();
                }
            }
            return delivered;
        }

        @Override public void onSubscribe(Subscription s) {
            assert loop.inLoopThread() : "not in event loop thread";
            assert delegate != null : "subscribed before setting delegate";
            subscribed = true;
            delegate.onSubscribe(s);
            flushOverDelivered();
        }

        private boolean checkActive(String method, @Nullable Object arg) {
            if (terminationState == TerminationState.NONE)
                return true;
            if (!checkActiveFailed) {
                checkActiveFailed = true;
                log.error("{}.{}({}): ignoring due to previous {}({})", this, method, arg,
                          error == null ? "onComplete" : "onError", error == null ? "" : error);
            } else {
                log.trace("{}.{}({}): ignoring due to previous {}({})", this, method, arg,
                          error == null ? "onComplete" : "onError", error == null ? "" : error);
            }
            return false;
        }

        @Override public void onNext(T item) {
            assert loop.inLoopThread() : "not in event loop thread";
            if (!checkActive("onNext", item))
                return;
            if (subscribed && requested > 0) {
                assert delegate != null : "subscribed, but null delegate";
                --requested;
                if (requested == 0) {
                    log.trace("{}.onBackpressure() upon receiving {}", name, item);
                    onBackpressure();
                }
                delegate.onNext(item);
            } else {
                overDelivered.add(item);
                log.trace("{}.onNext({}): buffered {}th item beyond requested",
                        name, item, overDelivered.size());
            }
        }

        @Override public void onError(Throwable cause) {
            assert loop.inLoopThread() : "not in event loop thread";
            if (checkActive("onError", cause)) {
                error = cause;
                int size = overDelivered.size();
                if (subscribed && size == 0) {
                    assert delegate != null : "subscribed, but null delegate";
                    terminationState = TerminationState.DELIVERED;
                    delegate.onError(cause);
                } else {
                    terminationState = TerminationState.PENDING;
                    log.trace("{}.onError({}): {} buffered items, delaying.", name, cause, size);
                }
            }
        }

        @Override public void onComplete() {
            assert loop.inLoopThread() : "not in event loop thread";
            if (checkActive("onComplete", "")) {
                error = null;
                int size = overDelivered.size();
                if (subscribed && size == 0) {
                    assert delegate != null : "subscribed, but null delegate";
                    terminationState = TerminationState.DELIVERED;
                    delegate.onComplete();
                } else {
                    terminationState = TerminationState.PENDING;
                    log.trace("{}.onComplete(): {} buffered items, delaying.", name, size);
                }
            }
        }
    }

    private static class PublisherAdapter<U> implements Publisher<U> {
        private @MonotonicNonNull CallbackPublisher2<U> outer;

        @Override public void subscribe(Subscriber<? super U> s) {
            assert outer != null : "outer not set";
            assert outer.loop.inLoopThread() : "not subscribing from the event loop thread";
            if (outer.subscriber.offerDelegate(s)) {
                outer.subscriber.onSubscribe(new Subscription() {
                    @Override public void request(long n) {
                        assert outer.loop.inLoopThread();
                        if (n < 0) {
                            outer.complete(new IllegalStateException("Negative request: " + n));
                        } else {
                            outer.requested += n;
                            n -= outer.subscriber.flushOverDelivered();
                            if (n > 0 && outer.terminationState == TerminationState.NONE)
                                outer.onRequest(n);
                        }
                    }

                    @Override public void cancel() {
                        assert outer.loop.inLoopThread();
                        if (outer.terminationState == TerminationState.NONE)
                            outer.onCancel();
                    }
                });
            } else {
                s.onSubscribe(new Subscription() {
                    @Override public void request(long n) {
                        log.error("Invalid subscription to {}, already subscribed", outer.name);
                    }
                    @Override public void cancel() {
                        log.error("Invalid subscription to {}, already subscribed", outer.name);
                    }
                });
                String msg = outer.name + " already has a subscriber: " + outer.subscriber;
                s.onError(new IllegalStateException(msg));
            }
        }
    }
}
