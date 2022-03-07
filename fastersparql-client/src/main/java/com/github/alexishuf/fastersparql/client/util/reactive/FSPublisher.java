package com.github.alexishuf.fastersparql.client.util.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * All publishers in fastersparql have a "home" executor. Such executor is where events other
 * than {@link Publisher#subscribe(Subscriber)} and {@link Subscriber#onSubscribe(Subscription)}
 * will be notified. That executor should be a single-thread executor, so that all
 * {@link FSPublisher}s bound to that executor have their events executed serially.
 *
 * As for the exceptions, they will occur on the thread that calls
 * {@link Publisher#subscribe(Subscriber)}, with {@link Subscriber#onSubscribe(Subscription)}
 * being called from within the {@link Publisher#subscribe(Subscriber)} call.
 *
 * The second rule for {@link FSPublisher}s is that clients of such publishers are
 * expected to make at most one {@link Publisher#subscribe(Subscriber)} call per
 * {@link FSPublisher} instance.
 *
 * There are two types of {@link FSPublisher} implementations:
 * <ol>
 *     <li><strong>processors</strong>, which simply wrap another {@link FSPublisher}</li>
 *     <li><strong>wrappers</strong>, which implement the events-in-executor rule.</li>
 * </ol>
 *
 * The two wrapper implementations available are {@link CallbackPublisher} and
 * {@link ExecutorBoundPublisher}. The later simply wraps a regular {@link Publisher} and ensures
 * that ann {@link Subscription} and {@link Subscriber} methods (except
 * {@link Subscriber#onSubscribe(Subscription)}) run serially in the given executor.
 * {@link CallbackPublisher} also implements serial execution of events in the given executor,
 * but allows non-reactive code to push ({@link CallbackPublisher#feed(Object)} items and complete
 * ({@link CallbackPublisher#complete(Throwable)} the stream asynchronously from multiple
 * unrelated threads.
 *
 * An executor-bound publisher processes all subscription events and delivers all subscriber events
 * from a task that always run in the same {@link Executor}.
 *
 * Thus, interacting with the {@link Publisher} and the {@link Subscription} delivered via
 * {@link Subscriber#onSubscribe(Subscription)} after a {@link Publisher#subscribe(Subscriber)}
 * is a thread-safe operation that merely queues the actual processing within the {@link Executor}.
 *
 * Likewise, all {@link Subscriber} events are delivered from within the Executor, serially.
 */
public interface FSPublisher<T> extends Publisher<T> {
    /**
     * Change the {@link Executor} where events triggered by {@link Subscription}s and callbacks
     * to {@link Subscriber}s are processed.
     *
     * @param executor the new {@link Executor} where events shall be processed.
     * @throws IllegalStateException if called after {@link Publisher#subscribe(Subscriber)}
     *         has been called.
     */
    void moveTo(Executor executor);

    /**
     * Get the executor currently used by this {@link Publisher}
     */
    Executor executor();

    /**
     * Return an {@link Iterable} wrapping this {@link Publisher}.
     *
     * {@link Publisher#subscribe(Subscriber)} will be called at {@link Iterator#hasNext()}
     * if {@link IterableAdapter#start()} is not explicitly called. At that point, if
     * {@link Publisher#subscribe(Subscriber)} had been previously called, no items will be
     * iterated and {@link IterableAdapter#error()} will return an {@link IllegalStateException}.
     *
     * Note that any errors produced by the {@link Publisher} are exposed via
     * {@link IterableAdapter#error()} and iteration of the {@link IterableAdapter} will silently
     * end after an error is delivered.
     *
     * @return an {@link IterableAdapter} over this reactive stream.
     */
    default IterableAdapter<T> asIterable() {
        return new IterableAdapter<>(this);
    }

    /**
     * Return a {@link Stream} over published items.
     *
     * This will cause a {@link Publisher#subscribe(Subscriber)} (and {@link FSPublisher}
     * only allow a single subscription on their lifecycle).
     *
     * Warning: Reading the reactive stream as a java stream <strong>will silently ignore any
     * errors emitted by the publisher</strong>.
     *
     * @return A {@link Stream} of items produced by this {@link Publisher}.
     * @throws IllegalStateException If this method, {@link Publisher#subscribe(Subscriber)}
     *                                or {@link FSPublisher#asIterable()} have been
     *                                previously called.
     */
    default Stream<T> asStream() {
        Iterator<T> it = asIterable().iterator();
        Spliterator<T> split = Spliterators.spliteratorUnknownSize(it, Spliterator.IMMUTABLE);
        return StreamSupport.stream(split, false);
    }

    /**
     * Get a {@link FSPublisher} for the stream produced by {@code publisher}.
     *
     * If {@code publisher} already is a {@link FSPublisher} it will be moved to the
     * given executor and returned (no wrapper is created).
     *
     * @param publisher the publisher to wrap if not already an {@link FSPublisher}.
     * @param executor the executor where {@link Subscription} and {@link Subscriber} methods
     *                 will be serially executed
     * @param <U> the type produced by the publisher.
     * @return the publisher itself or a wrapper.
     */
    static <U> FSPublisher<U> bind(Publisher<U> publisher, Executor executor) {
        if (publisher instanceof FSPublisher) {
            FSPublisher<U> bound = (FSPublisher<U>) publisher;
            bound.moveTo(executor);
            return bound;
        }
        return new ExecutorBoundPublisher<>(publisher, executor);
    }

    /**
     * Similar to {@link FSPublisher#bind(Publisher, Executor)}, but will
     * not {@link FSPublisher#moveTo(Executor)} if {@code publisher} already
     * is a {@link FSPublisher} and will use a random executor from
     * {@link BoundedEventLoopPool#get()} if {@code publisher} is not a {@link FSPublisher}.
     *
     * @param publisher the publisher to bind to a random executor if not a {@link FSPublisher}.
     * @param <R> the type of items produced by {@code publisher}
     * @return either {@code publisher}, unmodified or a {@link ExecutorBoundPublisher} wrapping it.
     */
    static <R> FSPublisher<R> bindToAny(Publisher<R> publisher) {
        if (publisher instanceof FSPublisher)
            return (FSPublisher<R>)publisher;
        return new ExecutorBoundPublisher<>(publisher, BoundedEventLoopPool.get().chooseExecutor());
    }
}
