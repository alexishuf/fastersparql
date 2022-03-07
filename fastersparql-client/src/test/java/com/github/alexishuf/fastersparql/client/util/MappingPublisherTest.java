package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MappingPublisher;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.Throwing.identity;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MappingPublisherTest {
    private static final int LARGE = FasterSparqlProperties.reactiveQueueCapacity() + 16;

    @AllArgsConstructor @Data @Accessors(fluent = true, chain = true)
    private static class Spec {
        int size;
        boolean offloadSubscribe, offloadPublish, dropFailed;

        public Flux<Integer> createFlux() {
            Flux<Integer> flux = Flux.range(1, size);
            if (offloadSubscribe)
                flux = flux.subscribeOn(Schedulers.boundedElastic());
            if (offloadPublish)
                flux = flux.publishOn(Schedulers.boundedElastic());
            return flux;
        }

        public <T> MappingPublisher<Integer, T>
        createPublisher(Throwing.Function<Integer, T> function) {
            FSPublisher<Integer> pub = FSPublisher.bindToAny(createFlux());
            return new MappingPublisher<>(pub, dropFailed, function);
        }

        public <T> List<T> createExpected(Throwing.Function<Integer, T> function) {
            return IntStream.range(1, size+1).boxed().map(i -> {
                try {
                    return function.apply(i);
                } catch (Throwable t) {
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }

        public boolean hasOffload() {
            return offloadSubscribe || offloadPublish;
        }
    }

    static Stream<Arguments> noDropSpecs() {

        List<Arguments> list = new ArrayList<>();
        boolean drop = false;
        for (Integer size : asList(0, 1, 2, 3, 4, 8, 16, LARGE)) {
            list.add(arguments(new Spec(size, false, false, drop)));
        }
        for (Integer size : asList(0, 1, 2, LARGE)) {
            list.add(arguments(new Spec(size, true, false, drop)));
            list.add(arguments(new Spec(size, true, false, drop)));
            list.add(arguments(new Spec(size, true, true, drop)));
        }
        return list.stream();
    }

    static Stream<Arguments> fewNoDropSpecs() {
        List<Spec> list = new ArrayList<>();
        for (Integer size : asList(0, 1, LARGE)) {
            list.add(new Spec(size, false, false, false));
            list.add(new Spec(size, true,  false, false));
            list.add(new Spec(size, false, true,  false));
        }
        return list.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("noDropSpecs")
    void testConsumeAll(Spec spec) {
        MappingPublisher<Integer, Integer> pub = spec.createPublisher(i -> i*-1);
        List<Integer> expected = spec.createExpected(i -> i * -1);
        assertEquals(expected, Flux.from(pub).collectList().block());
    }

    @ParameterizedTest @MethodSource("fewNoDropSpecs")
    void testCancelBeforeRequest(Spec spec) throws Throwable {
        MappingPublisher<Integer, Integer> pub = spec.createPublisher(i -> i * -1);
        List<Throwable> errors = new ArrayList<>();
        Semaphore subscribed = new Semaphore(0);
        pub.subscribe(new Subscriber<Integer>() {
            @Override public void onSubscribe(Subscription s) {
                for (int i = 0; i < 8; i++)
                    s.cancel(); //idempotent operation
                subscribed.release();
            }
            @Override public void onNext(Integer integer) {
                errors.add(new IllegalStateException("onNext() called"));
            }
            @Override public void onError(Throwable t) {
                errors.add(new IllegalStateException("onError() called"));
            }
            @Override public void onComplete() {
                if (spec.size > 0)
                    errors.add(new IllegalStateException("onComplete() called"));
            }
        });
        subscribed.acquireUninterruptibly();
        Thread.sleep(50);
        if (!errors.isEmpty())
            throw errors.get(0);
    }

    @Test
    void testSanity() {
        Spec spec = new Spec(0, false, false, false);
        assertEquals(Collections.emptyList(), spec.createFlux().collectList().block());
        assertEquals(Collections.emptyList(),
                     Flux.from(spec.createPublisher(identity())).collectList().block());
        assertEquals(Collections.emptyList(), spec.createExpected(identity()));
    }

    @ParameterizedTest @MethodSource("fewNoDropSpecs")
    void testCancelAfterFirstItemUnitRequest(Spec spec) throws Throwable {
        doTestCancelAfterFirstItem(spec, 1, true);
    }

    @ParameterizedTest @MethodSource("fewNoDropSpecs")
    void testCancelAfterFirstItemHugeRequest(Spec spec) throws Throwable {
        doTestCancelAfterFirstItem(spec, 65536, true);
    }

    @ParameterizedTest @MethodSource("fewNoDropSpecs")
    void testCancelAfterFirstItemHugeRequestNotReentrant(Spec spec) throws Throwable {
        doTestCancelAfterFirstItem(spec, 65536, false);
    }

    private void doTestCancelAfterFirstItem(Spec spec, int requestSize,
                                            boolean reentrant) throws Throwable {
        MappingPublisher<Integer, Integer> pub = spec.createPublisher(i -> i * -1);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
        CompletableFuture<Integer> first = new CompletableFuture<>();
        CompletableFuture<Void> complete = new CompletableFuture<>();
        List<Throwable> errors = new ArrayList<>();
        pub.subscribe(new Subscriber<Integer>() {
            private Subscription subscription;

            @Override public void onSubscribe(Subscription s) {
                subscriptionFuture.complete(subscription = s);
                s.request(requestSize);
            }

            @Override public void onNext(Integer integer) {
                if (!first.isDone())
                    first.complete(1);
                if (reentrant) {
                    subscription.cancel();
                    cancelled.set(true);
                }
            }

            @Override public void onError(Throwable t) {
                errors.add(new IllegalStateException("onError called", t));
            }

            @Override public void onComplete() {
                if (spec.size > 0 && cancelled.get())
                    errors.add(new IllegalStateException("onComplete called after cancel"));
                else
                    complete.complete(null);
            }
        });
        if (spec.size > 0) {
            assertEquals(1, first.get());
            if (!reentrant) {
                subscriptionFuture.get().cancel();
                cancelled.set(true);
            }
        } else {
            assertNotNull(subscriptionFuture.get());
            assertFalse(cancelled.get());
            assertNull(complete.get());
        }
        if (!errors.isEmpty())
            throw errors.get(0);
    }

    @ParameterizedTest @MethodSource("noDropSpecs")
    void testDropEven(Spec spec) {
        Throwing.Function<Integer, Integer> function = i -> {
            if (i % 2 == 0) throw new Exception("even number");
            return i * -1;
        };
        MappingPublisher<Integer, Integer> pub = spec.dropFailed(true).createPublisher(function);
        assertEquals(spec.createExpected(function), Flux.from(pub).collectList().block());
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2, 3})
    void testReportUpstreamError(int flags) throws Throwable {
        Flux<Integer> upstream = Flux.error(new Exception("oops"));
        if ((flags & 0x1) > 0)
            upstream = upstream.subscribeOn(Schedulers.boundedElastic());
        if ((flags & 0x2) > 0)
            upstream = upstream.publishOn(Schedulers.boundedElastic());

        MappingPublisher<Integer, Integer> pub;
        pub = new MappingPublisher<>(FSPublisher.bindToAny(upstream), false, identity());
        CompletableFuture<Subscription> subscriptionFuture = new CompletableFuture<>();
        CompletableFuture<Throwable> upstreamError = new CompletableFuture<>();
        List<Throwable> errors = new ArrayList<>();
        pub.subscribe(new Subscriber<Integer>() {
            @Override public void onSubscribe(Subscription s) {
                subscriptionFuture.complete(s);
                s.request(1);
            }
            @Override public void onNext(Integer i) {
                errors.add(new IllegalStateException("onNext("+i+") called"));
            }
            @Override public void onError(Throwable t) {
                if (!upstreamError.isDone())
                    upstreamError.complete(t);
                else
                    errors.add(new IllegalStateException("second onError() call with"+t));
            }
            @Override public void onComplete() {
                errors.add(new IllegalStateException("onComplete() called"));
            }
        });

        assertEquals(Exception.class, upstreamError.get().getClass());
        assertEquals("oops", upstreamError.get().getMessage());
        subscriptionFuture.get().cancel(); //no-op
        Thread.sleep(10);
        if (!errors.isEmpty())
            throw errors.get(0);
    }
}