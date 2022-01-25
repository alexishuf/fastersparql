package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.Throwing;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Implementation of {@link Processor} that applies a mapping function.
 *
 * @param <Input>> the input type
 * @param <Output>> the output type
 */
@Value @Accessors(fluent = true)
@AllArgsConstructor
@Slf4j
public class MappingPublisher<Input, Output> implements Publisher<Output> {
    /**
     * The source of input values for this {@link Publisher}.
     *
     * Each {@link MappingPublisher#subscribe(Subscriber)} call will cause a new
     * {@link Publisher#subscribe(Subscriber)} on the upstream {@link Publisher}.
     */
    Publisher<Input> upstreamPublisher;

    /**
     * If true, elements for which {@link MappingPublisher#mapFunction} throws are
     * dropped and not delivered to the downstream {@link Subscriber#onNext(Object)} method.
     * The exception will be logged on an INFO level, nevertheless.
     *
     * The default is false, so that {@link Throwable}s from {@link MappingPublisher#mapFunction}
     * are delivered to downstream {@link Subscriber#onError(Throwable)} and no further
     * elements are processed.
     */
    boolean dropFailed;

    /**
     * A function that converts instances of {@code Input} into instances of {@code Output}
     *
     * Such function may throw anything. The {@link Throwable} will be logged or delivered
     * to the downstream {@link Subscriber#onError(Throwable)} (stopping processing of
     * further elements), depending on the value of {@link MappingPublisher#dropFailed}.
     */
    Throwing.Function<Input, Output> mapFunction;

    public MappingPublisher(Publisher<Input> upstreamPublisher,
                            Throwing.Function<Input, Output> mapFunction) {
        this(upstreamPublisher, false, mapFunction);
    }

    /* --- --- --- method implementations --- --- --- */

    @Override public void subscribe(@NonNull Subscriber<? super Output> downstreamSubscriber) {
        Link<Input, Output> link = new Link<>(mapFunction, dropFailed, downstreamSubscriber);
        downstreamSubscriber.onSubscribe(link);
        if (!link.terminated)
            upstreamPublisher.subscribe(link);
    }

    /* --- --- --- implementation details --- --- --- */

    @Data
    private static class Link<T, R> implements Subscription, Subscriber<T> {
        private final Throwing.Function<T, R> mapFunction;
        private final boolean dropFailedMappings;
        private final Subscriber<? super R> downstream;
        private Subscription upstream;

        private boolean terminated;
        private long firstRequest;

        private void fail(Throwable throwable) {
            if (!terminated) {
                downstream.onError(throwable);
                if (upstream != null)
                    upstream.cancel();
                terminated = true;
            }
        }

        @Override public void onSubscribe(Subscription s) {
            if (upstream != null) {
                fail(new IllegalStateException("Link.onSubscribe() with old upstream "+upstream));
            } else {
                upstream = s;
                if (terminated) {
                    s.cancel();
                } else if (firstRequest > 0) {
                    upstream.request(firstRequest);
                } else if (firstRequest < 0) {
                    fail(new IllegalArgumentException("negative request(" + firstRequest + ")"));
                }
            }
        }

        @Override public void onNext(T input) {
            try {
                if (!terminated) downstream.onNext(mapFunction.apply(input));
            } catch (Throwable t) {
                if (!dropFailedMappings)
                    fail(t);
                else
                    log.info("Dropped {} since {} threw {}", input, mapFunction, t);
            }
        }

        @Override public void onError(Throwable t) {
            if (!terminated) {
                terminated = true;
                downstream.onError(t);
            }
        }

        @Override public void onComplete() {
            if (!terminated) {
                terminated = true;
                downstream.onComplete();
            }
        }

        @Override public void request(long n) {
            if (!terminated) {
                if (upstream == null) {
                    if (n < 0)
                        firstRequest = n;
                    else if (firstRequest >= 0)
                        firstRequest += n;
                } else {
                    upstream.request(n);
                }
            }
        }

        @Override public void cancel() {
            if (!terminated) {
                terminated = true;
                if (upstream != null)
                    upstream.cancel();
            }
        }
    }
}
