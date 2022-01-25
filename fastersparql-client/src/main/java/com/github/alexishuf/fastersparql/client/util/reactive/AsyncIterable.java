package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.exceptions.AsyncIterableCancelled;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A single-use cancellable {@link Iterable} over some asynchronous producer.
 *
 * If the producer dies, the iterator/spliterator/stream will not throw and will expose
 * whatever data was available before the producer failure and complete normally, without hanging.
 * To check if the producer has failed and the reason, use
 *
 * @param <T> the value type produced by the async producer and iterated here
 */
public interface AsyncIterable<T> extends Iterable<T>, AutoCloseable {
    /**
     * Signal the attached producer to start production, if it has not yet started.
     *
     * This method is idempotent. It will be implicitly called by {@link AsyncIterable#iterator()},
     * {@link AsyncIterable#spliterator()} and {@link AsyncIterable#stream()}.
     *
     * @return this {@link AsyncIterable} instance, for chaining other method calls.
     */
    AsyncIterable<T> start();

    /**
     * Get the error that caused the producer to fail, or null if the producer has not failed (yet).
     *
     * If this {@link AsyncIterable} has been completely iterated (including via {@code stream()}
     * and {@code spliterator()} methods), then the result of this method is final. That is, a
     * {@code null} indicates that the publisher has not failed and will not fail anymore.
     *
     * If {@link AsyncIterable#cancel()} has been called, this will return
     * {@link AsyncIterableCancelled}.
     *
     * @return The reason for the producer failure or null if it did not fail.
     */
    @Nullable Throwable error();

    /**
     * Whether an error has been flagged via {@link AsyncIterable#error()}.
     *
     * @return {@code true} iff {@link AsyncIterable#error()} {@code != null}.
     */
    @EnsuresNonNullIf(expression = "error()", result = true)
    default boolean hasError() { return error() != null; }

    /**
     * Notifies the attached asynchronous producer that no more items will be consumed.
     *
     * After this is called, {@link Iterator#hasNext()} will eventually return false.
     * As soon as this call returns, {@link AsyncIterable#error()} will return
     * {@link AsyncIterableCancelled}, unless the producer has previously failed, in which case
     * the producer failure reason will remain visible in {@link AsyncIterable#error()}.
     *
     * This method is idempotent and thread-safe.
     */
    void cancel();

    /**
     * Create a {@link Stream} over the produced values.
     *
     * @return a non-parallel Stream over produced values.
     *
     * @throws IllegalStateException if {@code iterator()}, {@code spliterator} or {@code stream}
     *                               have been already called on this instance.
     */
    default Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /* --- --- --- overridden methods --- --- --- */

    /**
     * Gets the {@link Iterable} over values produced by the {@link Publisher}.
     *
     * If {@link AsyncIterable#start()} has not yet been called, it will be called.
     *
     * This method can only be invoked once per {@link IterableAdapter} instance. The shared
     * queue does not keep a history. Thus, neither concurrent nor sequential iterators are
     * supported.
     *
     * @return a new {@link Iterator} over values produced by the {@link Publisher} given
     * on the constructor. The {@link Iterator} instance is not thread safe: if used from
     * multiple threads, those threads must synchronize.
     *
     * @throws IllegalStateException if {@code iterator()}, {@code spliterator} or {@code stream}
     *                               have been already called on this instance.
     */
    @Override Iterator<T> iterator();

    /**
     * See {@link Iterable#spliterator()}.
     *
     * Implementations should set at least the {@link Spliterator#ORDERED} and
     * {@link Spliterator#IMMUTABLE} flags.
     *
     * @return a new {@link Spliterator}. Like {@link AsyncIterable#iterator()},
     * @throws IllegalStateException if {@code iterator()}, {@code spliterator} or {@code stream}
     *                               have been already called on this instance.
     */
    @Override default Spliterator<T> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(),
                Spliterator.IMMUTABLE|Spliterator.ORDERED);
    }

    /**
     * Equivalent to {@link AsyncIterable#cancel()}.
     *
     * Unlike {@link AutoCloseable}, no exceptions should be thrown.
     */
    @Override default void close() {
        cancel();
    }
}
