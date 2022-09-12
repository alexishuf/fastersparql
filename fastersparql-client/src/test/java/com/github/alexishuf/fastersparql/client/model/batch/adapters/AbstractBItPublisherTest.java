package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static reactor.core.scheduler.Schedulers.boundedElastic;

public abstract class AbstractBItPublisherTest {

    protected record Scenario(int size, int minBatch, int minWaitMs, int maxBatch,
                              @Nullable Throwable error, Function<Scenario, BIt<Integer>> factory) {
        public BIt<Integer> createIt() { return factory.apply(this); }
        public List<Integer> expected() { return IntStream.range(0, size).boxed().toList(); }
    }

    public static Function<Scenario, BIt<Integer>> CALLBACK_FACTORY = new Function<>() {
        @Override public String toString() { return "CALLBACK_FACTORY"; }
        @Override public BIt<Integer> apply(Scenario scenario) {
            var it = new CallbackBIt<>(Integer.class, "CallbackBIt("+scenario+")");
            Thread.ofVirtual().start(() -> {
                for (int i = 0; i < scenario.size; i++)
                    it.feed(i);
                it.complete(scenario.error);
            });
            return it;
        }
    };

    public static Function<Scenario, BIt<Integer>> ITERATOR_FACTORY = new Function<>() {
        @Override public String toString() { return "ITERATOR_FACTORY"; }
        @Override public BIt<Integer> apply(Scenario scenario) {
            Iterator<Integer> plain = IntStream.range(0, scenario.size).boxed().iterator();
            var it = ThrowingIterator.andThrow(plain, scenario.error);
            return new IteratorBIt<>(it, Integer.class, "IteratorBIt("+scenario+")");
        }
    };

    public static Function<Scenario, BIt<Integer>> FLUX_FACTORY = new Function<>() {
        @Override public String toString() { return "FLUX_FACTORY"; }
        @Override public BIt<Integer> apply(Scenario scenario) {
            var flux = Flux.range(0, scenario.size);
            if (scenario.error != null)
                flux = Flux.concat(flux, Flux.error(scenario.error));
            var name = "PublisherBIt[Flux](" + scenario + ")";
            return new PublisherBIt<>(flux, Integer.class, name);
        }
    };

    public static Function<Scenario, BIt<Integer>> OFFLOADED_FLUX_FACTORY = new Function<>() {
        @Override public String toString() { return "OFFLOADED_FLUX_FACTORY"; }
        @Override public BIt<Integer> apply(Scenario scenario) {
            var flux = Flux.range(0, scenario.size)
                           .subscribeOn(boundedElastic()).publishOn(boundedElastic());
            if (scenario.error != null)
                flux = Flux.concat(flux, Flux.error(scenario.error));
            var name = "PublisherBIt[offloaded,Flux](" + scenario + ")";
            return new PublisherBIt<>(flux, Integer.class, name);
        }
    };

    public static List<Function<Scenario, BIt<Integer>>> FACTORIES
            = List.of(CALLBACK_FACTORY, ITERATOR_FACTORY, FLUX_FACTORY, OFFLOADED_FLUX_FACTORY);

    public static List<Scenario> scenarios() {
        List<Scenario> list = new ArrayList<>();
        for (int size : List.of(0, 1, 2, 9, 10, 128)) {
            for (int minBatch : List.of(1, 2, 65_536)) {
                for (int minWaitMs : List.of(0, 1, 50)) {
                    for (int maxBatch : List.of(65_536, minBatch)) {
                        for (Function<Scenario, BIt<Integer>> factory : FACTORIES) {
                            for (var error : Arrays.asList(null, new RuntimeException("test"))) {
                                list.add(new Scenario(size, minBatch, minWaitMs, maxBatch,
                                                      error, factory));
                            }
                        }
                    }
                }
            }
        }
        return list;
    }

    protected abstract void run(Scenario s);

    protected record DrainResult(List<Integer> list, @Nullable Throwable error) {
        public void assertExpected(Scenario s) {
            assertEquals(s.expected(), list);
            if (s.error == null) {
                if (error != null)
                    fail(error);
            } else if (error == null) {
                fail("Expected "+s.error+" to be thrown. Nothing was thrown");
            } else if (!error.equals(s.error) && error.getCause() != null) {
                assertEquals(s.error, error.getCause());
            } else {
                assertEquals(s.error, error);
            }
        }

    }

    protected static DrainResult fluxDrain(Publisher<Integer> publisher, Scenario scenario) {
        try {
            var list = Flux.from(publisher).collectList().block();
            return new DrainResult(list, null);
        } catch (Throwable t) {
            if (Objects.equals(t.getCause(),scenario.error))
                return new DrainResult(scenario.expected(), t.getCause());
            return new DrainResult(scenario.expected(), t);
        }
    }

    protected static DrainResult drain(Publisher<Integer> publisher, long requestSize) {
        List<Integer> items = new ArrayList<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Semaphore ready = new Semaphore(0);

        publisher.subscribe(new Subscriber<>() {
            private Subscription subscription;
            private long outstandingRequests = 0;
            private void fail(String msg) {
                error.compareAndSet(null, new AssertionFailedError(msg));
            }
            @Override public void onSubscribe(Subscription s) {
                if (subscription != null)
                    fail("Multiple onSubscribe()");
                subscription = s;
                outstandingRequests += requestSize;
                s.request(requestSize);
            }
            @Override public void onNext(Integer i) {
                if (subscription == null)
                    fail("onNext() before onSubscribe");
                if (ready.availablePermits() > 0)
                    fail("onNext() after completion");
                items.add(i);
                --outstandingRequests;
                if (outstandingRequests == 0) {
                    outstandingRequests += requestSize;
                    subscription.request(requestSize);
                }
            }
            @Override public void onError(Throwable t) {
                if (subscription == null)
                    fail("onError() before onSubscribe");
                if (ready.availablePermits() > 0)
                    fail("onError() after completion");
                error.compareAndSet(null, t);
                ready.release();
            }
            @Override public void onComplete() {
                if (subscription == null)
                    fail("onComplete() before onSubscribe");
                if (ready.availablePermits() > 0)
                    fail("onError() after completion");
                ready.release();
            }
        });
        ready.acquireUninterruptibly();
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return new DrainResult(items, error.get());
    }

    @Test
    void test() throws Exception {
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var task : scenarios().stream().map(s -> executor.submit(() -> run(s))).toList())
                task.get();
        }
    }
}
