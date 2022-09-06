package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PublisherBItTest extends AbstractBItTest {
    public interface PublisherFactory
            extends BiFunction<Integer, @Nullable Throwable, Publisher<Integer>> { }

    private static final PublisherFactory threadedPublisher = new PublisherFactory() {
        @Override public String toString() { return "ThreadedPublisher"; }

        @Override public Publisher<Integer> apply(Integer size, @Nullable Throwable error) {
            return new Publisher<>() {
                @Override public String toString() { return "ThreadedPublisher"; }
                @Override public void subscribe(Subscriber<? super Integer> s) {
                    Semaphore requests = new Semaphore(0);
                    AtomicBoolean cancelled = new AtomicBoolean();
                    Thread.ofVirtual().start(() -> {
                        int next = 0;
                        while (next < size) {
                            requests.acquireUninterruptibly();
                            if (cancelled.get()) return;
                            s.onNext(next++);
                        }
                        if (error == null)
                            s.onComplete();
                        else
                            s.onError(error);
                    });
                    s.onSubscribe(new Subscription() {
                        @Override public void request(long n) {
                            if (n > Integer.MAX_VALUE)
                                requests.release(Integer.MAX_VALUE);
                            else if (n < 0)
                                s.onError(new IllegalArgumentException("n < 0: " + n));
                            else if (n > 0)
                                requests.release((int) n);
                        }

                        @Override public void cancel() {
                            cancelled.set(true);
                        }
                    });
                }
            };
        }
    };

    private static final PublisherFactory recursivePublisher = new PublisherFactory() {
        @Override public String toString() { return "RecursivePublisher"; }
        @Override public Publisher<Integer> apply(Integer size, @Nullable Throwable error) {
            return new Publisher<>() {
                @Override public String toString() { return "ThreadedPublisher"; }
                @Override public void subscribe(Subscriber<? super Integer> s) {
                    s.onSubscribe(new Subscription() {
                        private int next = 0;
                        @Override public void request(long n) {
                            if (n < 0) {
                                s.onError(new IllegalArgumentException("n < 0: " + n));
                            } else {
                                for (int i = 0; i < n && next < size; i++)
                                    s.onNext(next++);
                                if (next == size) {
                                    if (error == null)
                                        s.onComplete();
                                    else
                                        s.onError(error);
                                    next++;
                                }
                            }
                        }

                        @Override public void cancel() {
                        }
                    });
                    if (size == 0) {
                        if (error == null)
                            s.onComplete();
                        else
                            s.onError(error);
                    }
                }
            };
        }
    };

    private static final PublisherFactory flux = new PublisherFactory() {
        @Override public String toString() { return "Flux"; }
        @Override public Publisher<Integer> apply(Integer size, @Nullable Throwable error) {
            return error == null ? Flux.range(0, size)
                                 : Flux.concat(Flux.range(0, size), Mono.error(error));
        }
    };

    private static final PublisherFactory offloadedSubscribeFlux = new PublisherFactory() {
        @Override public String toString() { return "OffloadedSubscribeFlux"; }
        @Override public Publisher<Integer> apply(Integer size, @Nullable Throwable error) {
            var p = error == null ? Flux.range(0, size)
                                  : Flux.concat(Flux.range(0, size), Mono.error(error));
            return p.subscribeOn(Schedulers.boundedElastic());
        }
    };

    private static final PublisherFactory offloadedPublishFlux = new PublisherFactory() {
        @Override public String toString() { return "OffloadedPublishFlux"; }
        @Override public Publisher<Integer> apply(Integer size, @Nullable Throwable error) {
            var p = error == null ? Flux.range(0, size)
                                  : Flux.concat(Flux.range(0, size), Mono.error(error));
            return p.publishOn(Schedulers.boundedElastic());
        }
    };

    private static final List<PublisherFactory> publisherFactories = List.of(
            recursivePublisher,
            threadedPublisher,
            flux,
            offloadedSubscribeFlux,
            offloadedPublishFlux
    );

    public static final class PublisherScenario extends Scenario {
        private final PublisherFactory publisherFactory;

        public PublisherScenario(Scenario other, PublisherFactory publisherFactory) {
            super(other);
            this.publisherFactory = publisherFactory;
        }

        public PublisherFactory publisherFactory() { return publisherFactory; }

        @Override public String toString() {
            return "PublisherScenario{size="+size +", minBatch="+minBatch+", maxBatch="+maxBatch
                    +", drainer="+drainer+", error="+error
                    +", publisherFactory="+publisherFactory+'}';
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof PublisherScenario that)) return false;
            if (!super.equals(o)) return false;
            return publisherFactory.equals(that.publisherFactory);
        }

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), publisherFactory);
        }
    }

    @Override protected List<? extends Scenario> scenarios() {
        List<PublisherScenario> list = new ArrayList<>();
        for (PublisherFactory factory : publisherFactories) {
            for (Scenario base : baseScenarios())
                list.add(new PublisherScenario(base, factory));
        }
        return list;
    }

    @Override protected void run(Scenario scenario) {
        PublisherScenario s = (PublisherScenario) scenario;
        Publisher<Integer> publisher = s.publisherFactory().apply(s.size(), s.error());
        try (PublisherBIt<Integer> bit = new PublisherBIt<>(publisher, Integer.class)) {
            s.drainer().drainOrdered(bit, s.expected(), s.error());
        }
    }

    static Stream<Arguments> selfTest() {
        List<Arguments> list = new ArrayList<>();
        for (PublisherFactory factory : publisherFactories) {
            for (int size : List.of(0, 1, 2)) {
                for (boolean error : List.of(false, true))
                    list.add(arguments(factory, size, error));
            }
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void selfTest(PublisherFactory publisherFactory, int size, boolean throwError) throws Exception {
        var exception = throwError ? new Exception("test") : null;
        CompletableFuture<List<Integer>> items = new CompletableFuture<>();
        CompletableFuture<Throwable> error = new CompletableFuture<>();
        publisherFactory.apply(size, exception).subscribe(new Subscriber<>() {
            private final List<Integer> list = new ArrayList<>();
            @Override public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
            @Override public void onNext(Integer i)           { list.add(i); }
            @Override public void onError(Throwable t)        { error.complete(t); items.complete(list); }
            @Override public void onComplete()                { items.complete(list); error.complete(null); }
        });
        assertEquals(IntStream.range(0, size).boxed().toList(), items.get());
        assertEquals(exception, error.get());
    }
}