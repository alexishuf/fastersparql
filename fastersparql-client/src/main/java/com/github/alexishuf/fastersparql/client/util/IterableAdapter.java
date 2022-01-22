package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.exceptions.AsyncIterableCancelled;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A single-use {@link Iterable} over a {@link Publisher}
 *
 * @param <T> the value type of the {@link Iterable}
 */
public class IterableAdapter<T> implements AsyncIterable<T> {
    private final BlockingQueue<Optional<T>> queue = new LinkedBlockingQueue<>();
    private final int capacity, requestSize;
    private int requested = 0;
    private final Publisher<? extends T> publisher;
    private @Nullable Subscription subscription;
    private boolean iterating, complete, closed;
    private Throwable error;

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
        this.requestSize = Math.max(capacity/2, 1);
        this.publisher = publisher;
    }

    private synchronized void enqueue(T value) {
        assert subscription != null;
        assert !complete;
        if (value == null) {
            error = new IllegalArgumentException(publisher+" produced a null value");
            subscription.cancel();
        } else {
            queue.add(Optional.of(value));
            --requested;
        }
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

    private synchronized void signalComplete(@Nullable Throwable t) {
        if (t != null) {
            if (error == null)
                error = t;
            else
                error.addSuppressed(t);
        }
        complete = true;
        queue.add(Optional.empty());
    }

    /**
     * Subscribes to the {@link Publisher}, starting production of values onto the shared queue.
     *
     * @return this {@link IterableAdapter} instance, for chaining of other methods.
     */
    @Override @SuppressWarnings("UnusedReturnValue")
    public synchronized IterableAdapter<T> start() {
        if (subscription != null)
            return this;
        if (closed)
            throw new IllegalStateException("close() already called for "+this);
        publisher.subscribe(new Subscriber<T>() {
            @Override public void onSubscribe(Subscription s) {
                subscription = s;
                ensureRequest();
            }
            @Override public void onNext(T t) {
                enqueue(t);
                ensureRequest();
            }
            @Override public void onError(Throwable t) {
                signalComplete(t);
            }
            @Override public void onComplete() {
                signalComplete(null);
            }
        });
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
        return "IterableAdapter{capacity=" + capacity + ", publisher=" + publisher + "}";
    }
}
