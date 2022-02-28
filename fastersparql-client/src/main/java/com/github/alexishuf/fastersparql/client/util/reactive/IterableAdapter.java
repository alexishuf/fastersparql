package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.exceptions.AsyncIterableCancelled;
import com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A single-use {@link Iterable} over a {@link Publisher}
 *
 * @param <T> the value type of the {@link Iterable}
 */
public class IterableAdapter<T> implements AsyncIterable<T> {
    private static final Logger log = LoggerFactory.getLogger(IterableAdapter.class);
    private final BlockingQueue<Optional<T>> queue;
    private final int capacity, requestSize;
    private int requested = 0;
    private final Publisher<? extends T> publisher;
    private @Nullable Subscription subscription;
    private final Subscriber<T> subscriber = new AdapterSubscriber();
    private boolean subscribed, iterating, closed;
    private final boolean complete = false;
    private volatile Throwable error = null;

    /**
     * Create a new {@link IterableAdapter} with default queue capacity.
     *
     * See {@link FasterSparqlProperties#reactiveQueueCapacity()} on how to override the
     * queue capacity default.
     *
     * @param publisher the source of values for the {@link Iterable}.
     */
    public IterableAdapter(Publisher<? extends T> publisher) {
        this(publisher, FasterSparqlProperties.reactiveQueueCapacity());
    }

    /**
     * Create a new {@link IterableAdapter} over values produced by the given {@link Publisher}.
     *
     * The iterator will iterate over elements in a queue of given {@code capacity} that
     * is concurrently fed by the {@link Publisher}.
     *
     * @param publisher the source of values
     * @param capacity the capacity of the queue where the {@code publisher} produces and
     *                 this {@link Iterable} consumes.
     */
    public IterableAdapter(Publisher<? extends T> publisher, @Positive int capacity) {
        this.capacity = capacity;
        this.queue = new ArrayBlockingQueue<>(capacity+8);
        this.requestSize = Math.max(capacity/2, 1);
        this.publisher = publisher;
    }

    private synchronized void ensureRequest() {
        if (subscription != null && !complete) {
            int free = capacity - (queue.size() + requested);
            if (free >= requestSize) {
                requested += requestSize;
                subscription.request(requestSize);
            }
        }
    }

    private void signalComplete(@Nullable Throwable cause) {
        synchronized (this) {
            if (complete && !(cause instanceof AsyncIterableCancelled)
                    && !(error instanceof AsyncIterableCancelled)) {
                Object c = cause == null ? "onComplete" : "onError", ca = cause == null ? "" : cause;
                Object p = error == null ? "onComplete" : "onError", pa = error == null ? "" : error;
                log.warn("{}.{}({}): previous {}({})", this, c, ca, p, pa);
            }
            if (cause != null) {
                if (error == null) error = cause;
                else               error.addSuppressed(cause);
            }
        }
        queue.add(Optional.empty());
    }

    private class AdapterSubscriber implements Subscriber<T> {
        @Override public void onSubscribe(Subscription s) {
            synchronized (IterableAdapter.this) {
                subscription = s;
                IterableAdapter.this.notifyAll();
            }
            // release monitor to allow wait()ing start() to wake up
            ensureRequest();
        }
        @Override public void onNext(T t) {
            synchronized (IterableAdapter.this) {
                assert subscription != null;
                if (complete && !(error instanceof AsyncIterableCancelled)) {
                    log.error("{}.onNext({}) after {}({})", this, t,
                              error == null ? "onComplete" : "onError", error == null ? "" : error);
                } else if (t == null) {
                    subscription.cancel();
                    signalComplete(new IllegalArgumentException(publisher+" produced a null"));
                } else {
                    --requested;
                    if (requested == -1) {
                        log.warn("{}.onNext({}): element delivered beyond requested", this, t);
                    } else if (requested < -1) {
                        log.trace("{}.onNext({}): {}th element delivered beyond requested",
                                  this, t, Math.abs(requested));
                    }
                    queue.add(Optional.of(t));
                }
                ensureRequest();
            }
        }
        @Override public void onError(Throwable cause) {
            signalComplete(cause);
        }
        @Override public void onComplete() {
            signalComplete(null);
        }

        @Override public String toString() {
            return IterableAdapter.this.toString();
        }
    }

    /**
     * Subscribes to the {@link Publisher}, starting production of values onto the shared queue.
     *
     * @return this {@link IterableAdapter} instance, for chaining of other methods.
     */
    @Override @SuppressWarnings("UnusedReturnValue")
    public IterableAdapter<T> start() {
        synchronized (this) {
            if (subscribed)
                return this;
            if (closed)
                throw new IllegalStateException("close() already called for " + this);
            subscribed = true;
        }
        publisher.subscribe(subscriber);
        synchronized (this) {
            boolean interrupted = false;
            boolean waited = subscription == null;
            if (waited)
                log.trace("{}: async {}.subscribe(), waiting onSubscribe()", this, publisher);
            while (subscription == null) {
                try {
                    wait();
                } catch (InterruptedException e) { interrupted = true; }
            }
            if (waited)
                log.trace("{}: received onSubscribe({})", this, subscription);
            if (interrupted)
                Thread.currentThread().interrupt();
        }
        return this;
    }

    @Override public @Nullable Throwable error() {
        return error;
    }

    @Override public Iterator<T> iterator() {
        if (iterating)
            throw new IllegalStateException("iterator() already called for "+this);
        iterating = true;
        if (closed)
            throw new IllegalStateException(".close() already called for "+this);
        if (subscription == null)
            start();
        return new Iterator<T>() {
            private @Nullable T next = null;
            private boolean atEnd = false;

            @Override public boolean hasNext() {
                boolean interrupted = false;
                while (next == null && !atEnd) {
                    try {
                        next = queue.take().orElse(null);
                        atEnd = next == null;
                        if (!atEnd) ensureRequest();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted)
                    Thread.currentThread().interrupt();
                return next != null;
            }

            @Override public T next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                T next = this.next;
                this.next = null;
                return next;
            }
        };
    }

    /**
     * See {@link Iterable#spliterator()}.
     *
     * This implementation adds the immutable, ordered and non-null flags.
     *
     * @return a non-null spliterator wrapping {@link IterableAdapter#iterator()}.
     */
    @Override public Spliterator<T> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(),
                Spliterator.IMMUTABLE|Spliterator.ORDERED|Spliterator.NONNULL);
    }

    @Override public synchronized void cancel() {
        if (!closed) {
            closed = true;
            if (!complete && subscription != null)
                subscription.cancel();
            signalComplete(new AsyncIterableCancelled());
        }
    }

    @Override public String toString() {
        return String.format("IterableAdapter{capacity=%d, %scomplete, error=%s, publisher=%s}@%x",
                             capacity, complete ? "" : "!",
                             error == null ? "null" : error.getClass().getSimpleName(),
                             publisher, System.identityHashCode(this));
    }
}
