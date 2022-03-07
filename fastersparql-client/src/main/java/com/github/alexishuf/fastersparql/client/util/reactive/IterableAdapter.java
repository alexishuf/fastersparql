package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.exceptions.AsyncIterableCancelled;
import com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A single-use {@link Iterable} over a {@link Publisher}
 *
 * @param <T> the value type of the {@link Iterable}
 */
public class IterableAdapter<T> implements AsyncIterable<T> {
    private static final Logger log = LoggerFactory.getLogger(IterableAdapter.class);

    /* --- --- --- immutable state --- --- --- */
    private final FSPublisher<? extends T> publisher;
    private final ArrayDeque<T> queue;
    private final long capacity, requestSize;

    /* --- --- --- mutable state --- --- --- */
    private @MonotonicNonNull It it;
    private @MonotonicNonNull Throwable cause;
    private @MonotonicNonNull Subscription subscription;
    private boolean terminated, cancelled;

    /* --- --- --- constructors --- --- --- */

    public IterableAdapter(Publisher<? extends T> upstreamPublisher) {
        this(upstreamPublisher, FasterSparqlProperties.reactiveQueueCapacity());
    }

    public IterableAdapter(Publisher<? extends T> publisher, @Positive int capacity) {
        this.publisher = FSPublisher.bindToAny(publisher);
        this.requestSize = Math.min(Integer.MAX_VALUE/2 - 8, Math.max(capacity/2, 1));
        this.capacity = requestSize * 2;
        this.queue = new ArrayDeque<>(capacity);
    }

    /* --- ---- --- public interface --- --- --- */

    @Override public @Nullable Throwable error() {
        return cause;
    }

    @Override synchronized public void cancel() {
        if (!cancelled) {
            cancelled = true;
            if (!terminated) {
                cause = new AsyncIterableCancelled();
                if (subscription != null)
                    subscription.cancel();
            }
            notifyAll();
        }
    }

    @Override public Iterator<T> iterator() {
        if (it != null || cancelled)
            throw new IllegalStateException("iterator() already called");
        return it = new It();
    }

    @Override synchronized public IterableAdapter<T> start() {
        if (!cancelled && subscription == null) {
            publisher.subscribe(subscriber);
            assert subscription != null : "onSubscribe() not called directly";
        }
        return this;
    }

    /* --- ---- --- iterator/subscriber logic --- --- --- */

    private final class It implements Iterator<T> {
        private @MonotonicNonNull T next;
        boolean calledStart = false, reportedEnd;

        @EnsuresNonNullIf(result = true, expression = "this.next")
        @Override public boolean hasNext() {
            if (!calledStart) {
                calledStart = true;
                start();
            }
            if (reportedEnd)
                return false;
            else if (next != null)
                return true;
            synchronized (IterableAdapter.this) {
                boolean interrupted = false;
                while (!terminated && !cancelled && queue.isEmpty()) {
                    try {
                        IterableAdapter.this.wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                boolean has = !queue.isEmpty();
                if (has)
                    next = queue.removeFirst();
                else
                    reportedEnd = true;
                if (interrupted)
                    Thread.currentThread().interrupt();
                return has;
            }
        }

        @Override public T next() {
            if (!hasNext()) throw new NoSuchElementException();
            assert this.next != null;
            T next = this.next;
            this.next = null;
            return next;
        }
    }

    private final Subscriber<T> subscriber = new Subscriber<T>() {
        private long requested = 0;

        @Override public void onSubscribe(Subscription s) {
            subscription = s;
            s.request(requested = requestSize*2);
        }

        @Override public void onNext(T item) {
            long free;
            synchronized (IterableAdapter.this) {
                if (requested == 0) {
                    log.warn("{} received onNext() beyond requested", this);
                    assert false : "onNext() beyond requested";
                } else {
                    --requested;
                }
                free = cancelled || terminated ? 0 : capacity - requested;
                if (free >= requestSize) {
                    requested += requestSize;
                    subscription.request(requestSize);
                }
                if (terminated) {
                    log.warn("{}.onNext({}) after {}({})", this, item,
                            cause == null ? "onComplete" : "onError",
                            cause == null ? "" : cause.toString());
                    assert false : "onNext() after onComplete()/onNext()";
                }
                boolean changedWaitCondition = queue.isEmpty();
                queue.add(item);
                if (changedWaitCondition)
                    IterableAdapter.this.notifyAll();
            }
        }

        @Override public void onError(Throwable t) {
            synchronized (IterableAdapter.this) {
                if (terminated) {
                    log.info("{}.onError({}): double termination, previous {}({})", this, t,
                            cause == null ? "onComplete" : "onError", cause == null ? "" : cause);
                } else {
                    log.trace("{}.onError({})", this, Objects.toString(t));
                    terminated = true;
                    cause = t;
                    IterableAdapter.this.notifyAll();
                }
            }
        }

        @Override public void onComplete() {
            synchronized (IterableAdapter.this) {
                if (terminated) {
                    log.info("{}.onComplete(): double termination, previous {}({})", this,
                            cause == null ? "onComplete" : "onError", cause == null ? "" : cause);
                } else {
                    log.trace("{}.onComplete()", this);
                    terminated = true;
                    IterableAdapter.this.notifyAll();
                }
            }
        }

        @Override public String toString() {
            return IterableAdapter.this + ".subscriber";
        }
    };

    @Override public String toString() {
        return "Iterable("+publisher+")";
    }
}
