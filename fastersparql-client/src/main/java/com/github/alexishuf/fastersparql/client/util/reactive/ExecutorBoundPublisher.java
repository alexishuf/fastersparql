package com.github.alexishuf.fastersparql.client.util.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.Executor;

/**
 * An executor-bound publisher processes all subscription events and delivers all subscriber events
 * from a task that always run in the same {@link Executor}.
 *
 * Thus, interacting with the {@link Publisher} and the {@link Subscription} delivered via
 * {@link Subscriber#onSubscribe(Subscription)} after a {@link Publisher#subscribe(Subscriber)}
 * is a thread-safe operation that merely queues the actual processing within the {@link Executor}.
 *
 * Likewise, all {@link Subscriber} events are delivered from within the Executor, serially.
 */
public interface ExecutorBoundPublisher<T> extends Publisher<T> {
    /**
     * Change the {@link Executor} where events triggered by {@link Subscription}s and callbacks
     * to {@link Subscriber}s are processed.
     *
     * The change is asynchronous:  eventually, events will be processed in the given
     * {@link Executor}. However, if this method is called before the first
     * {@link Publisher#subscribe(Subscriber)} call, there is no event queued and thus the change
     * will be complete once this method returns.
     *
     * @param executor the new {@link Executor} where events shall be processed.
     */
    void moveTo(Executor executor);

    /**
     * Get the executor currently used by this {@link Publisher}
     */
    Executor executor();

    static <U> ExecutorBoundPublisher<U> bind(Publisher<U> publisher, Executor executor) {
        if (publisher instanceof ExecutorBoundPublisher) {
            ExecutorBoundPublisher<U> bound = (ExecutorBoundPublisher<U>) publisher;
            bound.moveTo(executor);
            return bound;
        }
        return new ExecutorBoundPublisherWrapper<>(publisher, executor);
    }
}
