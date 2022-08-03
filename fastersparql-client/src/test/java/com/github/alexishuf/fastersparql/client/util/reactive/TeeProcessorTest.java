package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.CompletableAsyncTask;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.val;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.async.Async.async;
import static com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher.bindToAny;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TeeProcessorTest {
    private interface TeeConsumer extends Function<Publisher<Integer>, List<Integer>> {}

    private static final TeeConsumer FLUX_CONSUMER = new TeeConsumer() {
        @Override public List<Integer> apply(Publisher<Integer> tee) {
            return Flux.from(tee).collectList().block();
        }
        @Override public String toString() { return "FLUX_CONSUMER"; }
    };

    @RequiredArgsConstructor
    private static class BatchConsumer implements TeeConsumer {
        private final int batch;

        @Override public List<Integer> apply(Publisher<Integer> tee) {
            CompletableAsyncTask<List<Integer>> f = new CompletableAsyncTask<>();
            tee.subscribe(new Subscriber<Integer>() {
                private final List<Integer> list = new ArrayList<>();
                private @MonotonicNonNull Subscription up;
                @Override public void onSubscribe(Subscription s) { (up = s).request(batch); }
                @Override public void onNext(Integer integer) {
                    list.add(integer);
                    requireNonNull(up).request(batch);
                }
                @Override public void onError(Throwable t) { assertTrue(f.completeExceptionally(t)); }
                @Override public void onComplete()         { assertTrue(f.complete(list)); }
            });
            return f.fetch();
        }
    }

    private static final TeeConsumer UNIT_CONSUMER = new BatchConsumer(1) {
        @Override public String toString() { return "UNIT_CONSUMER"; }
    };

    private static final TeeConsumer BATCH_CONSUMER = new BatchConsumer(4) {
        @Override public String toString() { return "BATCH_CONSUMER(4)"; }
    };

    private static final TeeConsumer SINGLE_REQ_CONSUMER = new TeeConsumer() {
        @Override public List<Integer> apply(Publisher<Integer> tee) {
            List<Integer> list = new ArrayList<>();
            CompletableAsyncTask<List<Integer>> future = new CompletableAsyncTask<>();
            tee.subscribe(new Subscriber<Integer>() {
                @Override public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
                @Override public void onNext(Integer i)           { list.add(i); }
                @Override public void onError(Throwable t)        { assertTrue(future.completeExceptionally(t)); }
                @Override public void onComplete()                { assertTrue(future.complete(list)); }
            });
            return future.fetch();
        }
        @Override public String toString() { return "SINGLE_REQ_CONSUMER"; }
    };
    private static final List<TeeConsumer> CONSUMERS
            = asList(SINGLE_REQ_CONSUMER, UNIT_CONSUMER, BATCH_CONSUMER, FLUX_CONSUMER);

    @Value
    private static class AfterSubscribe<T> implements Publisher<T> {
        Publisher<T> delegate;
        Runnable runnable;

        @Override public void subscribe(Subscriber<? super T> s) {
            delegate.subscribe(s);
            runnable.run();
        }
    }

    @Value @Accessors(fluent = true)
    private static class TestData {
        @ToString.Exclude Supplier<Flux<Integer>> source;
        int repetition, consumers;
        TeeConsumer consumer;

        @ToString.Include List<Integer> expected() {
            return requireNonNull(source.get().collectList().block());
        }
    }

    static Stream<Arguments> test() {
        List<TestData> scenarios = new ArrayList<>();
        IntStream.of(2, 1, 0, 16, 1024).forEach(size -> {
            Supplier<Flux<Integer>> supplier = () -> Flux.range(0, size);
            for (val consumer : CONSUMERS) {
                for (Integer nConsumers : asList(1, 2, 3, 16)) {
                    for (int repetition = 0; repetition < 4; repetition++)
                        scenarios.add(new TestData(supplier, nConsumers, repetition, consumer));
                }
            }
        });
        return scenarios.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(TestData ts) {
        val subscribed = new CountDownLatch(ts.consumers);
        val rawTee = new TeeProcessor<>(bindToAny(ts.source.get()));
        val tee = new AfterSubscribe<>(rawTee, subscribed::countDown);
        val ex = ts.expected();
        List<AsyncTask<?>> tasks = new ArrayList<>();
        tasks.add(async(() -> {
            while (true) {
                try {
                    subscribed.await();
                    rawTee.start();
                    break;
                } catch (InterruptedException ignored) {}
            }
        }));
        for (int i = 0; i < ts.consumers; i++)
            tasks.add(async(() -> assertEquals(ex, ts.consumer.apply(tee), toString())));
        for (AsyncTask<?> task : tasks)
            task.fetch();
    }

    static Stream<Arguments> testSingleConsumer() {
        List<Arguments> list = new ArrayList<>();
        for (val consumer : CONSUMERS)
            Stream.of(0, 1, 2, 32).forEach(i -> list.add(arguments(consumer, i)));
        return list.stream();
    }

    @Timeout(10) @ParameterizedTest @MethodSource
    void testSingleConsumer(TeeConsumer consumer, int size) {
        val tee = new TeeProcessor<>(bindToAny(Flux.range(0, size))).start();
        assertEquals(range(0, size).boxed().collect(toList()),
                     consumer.apply(tee));
    }


    @Timeout(10) @ParameterizedTest @ValueSource(ints = {0, 1, 4})
    void testCanStartAfterSubscribe(int size) throws ExecutionException, InterruptedException {
        val tee = new TeeProcessor<>(bindToAny(Flux.range(0, size)));
        CompletableFuture<List<Integer>> list = new CompletableFuture<>();
        Semaphore subscribed = new Semaphore(0);
        tee.subscribe(new Subscriber<Integer>() {
            private Subscription up;
            private final List<Integer> items = new ArrayList<>();
            @Override public void onSubscribe(Subscription s) {
                (up = s).request(1);
                subscribed.release();
            }
            @Override public void onNext(Integer i)    { items.add(i); up.request(1);  }
            @Override public void onError(Throwable t) { list.completeExceptionally(t); }
            @Override public void onComplete()         { list.complete(items); }
        });
        assertFalse(list.isDone());
        subscribed.acquireUninterruptibly();
        Thread.sleep(10);
        assertFalse(list.isDone());

        tee.start();
        assertEquals(range(0, size).boxed().collect(toList()),
                     list.get());
    }

    @Timeout(10) @Test
    void testErrorSourceSingleConsumer() {
        RuntimeException ex = new RuntimeException("test");
        for (val consumer : CONSUMERS) {
            try {
                val tee = new TeeProcessor<Integer>(bindToAny(Flux.error(ex))).start();
                consumer.apply(tee);
                fail("Expected "+ex+" to be thrown");
            } catch (Throwable t) {
                assertTrue(t == ex || t.getCause() == ex, "unexpected exception: "+t);
            }
        }
    }

    @Timeout(10) @Test
    void testErrorSource() {
        RuntimeException ex = new RuntimeException("test");
        val tee = new TeeProcessor<Integer>(bindToAny(Flux.error(ex))).start();
        for (val consumer : CONSUMERS) {
            try {
                consumer.apply(tee);
                fail("Expected "+ex+" to be thrown");
            } catch (Throwable t) {
                assertTrue(t == ex || t.getCause() == ex, "unexpected exception: "+t);
            }
        }
    }

    @Timeout(20) @ParameterizedTest @ValueSource(ints = {0, 1, 3, 16, 32, 1024})
    void testDelayErrors(int size) throws InterruptedException {
        val exception = new RuntimeException("test");
        val flux = bindToAny(Flux.concat(Flux.range(0, size), Flux.error(exception)));
        val tee = new TeeProcessor<>(flux).start();
        List<Integer> actual = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();
        Semaphore ready = new Semaphore(0);
        tee.subscribe(new Subscriber<Integer>() {
            private Subscription up;
            private boolean terminated;
            @Override public void onSubscribe(Subscription s) {
                assertNull(up);
                assertNotNull(s);
                (up = s).request(1);
            }
            @Override public void onNext(Integer i) {
                actual.add(i);
                up.request(1);
            }
            @Override public void onError(Throwable t) {
                errors.add(t);
                terminated = true;
                ready.release();
            }
            @Override public void onComplete() {
                if (terminated)
                    errors.add(new IllegalStateException("onComplete after onError"));
                terminated = true;
                ready.release();
            }
        });
        ready.acquireUninterruptibly(); // wait for onError/onComplete
        assertEquals(range(0, size).boxed().collect(toList()), actual);
        assertEquals(singletonList(exception), errors);

        // test no events arrive after onError/onComplete
        Thread.sleep(10);
        assertEquals(range(0, size).boxed().collect(toList()), actual);
        assertEquals(singletonList(exception), errors);
    }

    @Test
    void testSkipLostItems() throws ExecutionException, InterruptedException {
        val source = bindToAny(Flux.range(0, 4));
        val tee = new TeeProcessor<>(source).start();

        // consume two items
        CompletableFuture<Subscription> subscription = new CompletableFuture<>();
        CompletableFuture<Integer> two = new CompletableFuture<>();
        CompletableFuture<Integer> four = new CompletableFuture<>();
        tee.subscribe(new Subscriber<Integer>() {
            private int received = 0;
            @Override public void onSubscribe(Subscription s) { subscription.complete(s); }
            @Override public void onNext(Integer integer) {
                ++received;
                if (received == 2) two.complete(received);
            }
            @Override public void onError(Throwable t) {
                two.completeExceptionally(t);
                four.completeExceptionally(t);
            }
            @Override public void onComplete() {
                if (received == 4) four.complete(received);
                else               four.completeExceptionally(new IllegalStateException(">4 items"));
            }
        });
        subscription.get().request(2);
        assertEquals(2, two.get());

        // second subscriber lost 2 items
        CompletableFuture<List<Integer>> received = new CompletableFuture<>();
        tee.subscribe(new Subscriber<Integer>() {
            private final List<Integer> list = new ArrayList<>();
            @Override public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
            @Override public void onNext(Integer i)           { list.add(i); }
            @Override public void onError(Throwable t)        { received.completeExceptionally(t); }
            @Override public void onComplete()                { received.complete(list); }
        });
        assertEquals(asList(2, 3), received.get());

        // first subscriber not completed (it did not request()ed more items
        Thread.sleep(10);
        assertFalse(four.isDone());

        // first subscribers completes from history
        subscription.get().request(2);
        assertEquals(4, four.get());
    }

    @Test
    void testLostAllItems() {
        val source = bindToAny(Flux.range(0, 2));
        val tee = new TeeProcessor<>(source).errorOnLostItems().start();
        assertEquals(asList(0, 1), SINGLE_REQ_CONSUMER.apply(tee));
        CompletableFuture<?> future = new CompletableFuture<>();
        tee.subscribe(new Subscriber<Integer>() {
            @Override public void onSubscribe(Subscription s) { }
            @Override public void onNext(Integer integer) {
                future.completeExceptionally(new AssertionFailedError("onNext() called"));
            }
            @Override public void onError(Throwable t) {
                if (t instanceof TeeProcessor.MissingHistoryException)
                    future.complete(null);
                else
                    future.completeExceptionally(t);
            }
            @Override public void onComplete() {
                future.completeExceptionally(new AssertionFailedError("normal onComplete()"));
            }
        });
    }

    @Test
    void testLostFirstItems() throws ExecutionException, InterruptedException {
        val source = bindToAny(Flux.range(0, 4));
        val tee = new TeeProcessor<>(source).errorOnLostItems().start();
        List<Throwable> failures = new ArrayList<>();

        // first consumer consumes 2/4 items
        CompletableFuture<Subscription> firstSubscription = new CompletableFuture<>();
        CompletableFuture<List<Integer>> twoItems = new CompletableFuture<>();
        CompletableFuture<List<Integer>> allItems = new CompletableFuture<>();
        tee.subscribe(new Subscriber<Integer>() {
            private final List<Integer> received = new ArrayList<>();
            @Override public void onSubscribe(Subscription s) {
                if (!firstSubscription.complete(s))
                    failures.add(new IllegalStateException("double onSubscribe"));
                s.request(2);
            }
            @Override public void onNext(Integer i) {
                received.add(i);
                if (received.size() == 2) {
                    twoItems.complete(received);
                } else if (received.size() == 4) {
                    allItems.complete(received);
                } else if (received.size() > 4) {
                    val ex = new IllegalStateException("More than 4 items received");
                    twoItems.completeExceptionally(ex);
                    failures.add(ex);
                }
            }
            @Override public void onError(Throwable t) {
                if (!twoItems.completeExceptionally(t) || !allItems.completeExceptionally(t))
                    failures.add(t);
            }
            @Override public void onComplete() {
                val ex = new IllegalStateException("Unexpected completion");
                if (!twoItems.completeExceptionally(ex) || !allItems.completeExceptionally(ex))
                    failures.add(ex);
            }
        });
        assertEquals(asList(0, 1), twoItems.get());
        assertEquals(emptyList(), failures);

        //second subscriber receives error
        CompletableFuture<TeeProcessor.MissingHistoryException> missingHistory = new CompletableFuture<>();
        tee.subscribe(new Subscriber<Integer>() {
            @Override public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
            @Override public void onNext(Integer i) {
                val ex = new IllegalStateException("Unexpected item: " + i);
                if (!missingHistory.completeExceptionally(ex))
                    failures.add(ex);
            }
            @Override public void onError(Throwable t) {
                if (t instanceof TeeProcessor.MissingHistoryException)
                    missingHistory.complete((TeeProcessor.MissingHistoryException) t);
                else if (!missingHistory.completeExceptionally(t))
                    failures.add(t);
            }

            @Override public void onComplete() {
                val ex = new IllegalStateException("Expected onError(), got onComplete");
                if (!missingHistory.completeExceptionally(ex))
                    failures.add(ex);
            }
        });
        assertNotNull(missingHistory.get());
        Thread.sleep(10);
        assertEquals(emptyList(), failures);

        // first subscriber can still complete normally
        firstSubscription.get().request(2);
        assertEquals(asList(0, 1, 2, 3), allItems.get());
    }

    @Test
    void testWithMergePublisher() {
        range(0, 4).mapToObj(i -> async(() -> doTestWithMergePublisher(i)))
                   .forEach(AsyncTask::fetch);
    }

    private void doTestWithMergePublisher(int runId) {
        int concurrency = Runtime.getRuntime().availableProcessors();
        val tee = new TeeProcessor<>(bindToAny(Flux.range(0, 1024)));
        val merge = new MergePublisher<Integer>("test", concurrency, concurrency,
                                                false, null);
        range(0, concurrency*2).forEach(i
                -> merge.addPublisher(new AbstractProcessor<Integer, Integer>(tee) {
                    @Override protected void handleOnNext(Integer i) { emit(i); }
                    @Override public String toString() { return "pipe-"+runId+"-"+i; }
                }));
        merge.markCompletable();
        tee.start();
        val expected = range(0, concurrency * 2).flatMap(i -> range(0, 1024))
                                                .boxed().collect(toSet());
        val actual = Flux.from(merge).collect(Collectors.toSet()).block();
        assertEquals(expected, actual);
    }

    static Stream<Arguments> testStartAfterSubscribers() {
        List<Arguments> list = new ArrayList<>();
        for (int size : asList(0, 1, 2, 3, 8, 1024)) {
            for (int subscribers : asList(1, 2, 3, 128))
                CONSUMERS.forEach(consumer -> list.add(arguments(size, subscribers, consumer)));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testStartAfterSubscribers(int size, int subscribers, TeeConsumer consumer) {
        int repetitions = size < 8 ? 100 : 10;
        for (int repetition = 0; repetition < repetitions; repetition++) {
            val expected = range(0, size).boxed().collect(toList());
            val tee = new TeeProcessor<>(bindToAny(Flux.range(0, size)));
            tee.startAfterSubscribedBy(subscribers);
            List<AsyncTask<?>> tasks = new ArrayList<>();
            for (int i = 0; i < subscribers; i++)
                tasks.add(async(() -> assertEquals(expected, consumer.apply(tee))));
            for (AsyncTask<?> task : tasks) task.fetch();
        }
    }
}