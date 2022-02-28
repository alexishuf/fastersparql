package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import lombok.Value;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MultiThreadedMergePublisherTest {
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors()*2;
    private static final int N_ITERATIONS = 3;
    private static final int QUEUE = 4;

    private void assertExpected(IterableAdapter<Integer> subscriber,
                                Class<? extends Throwable> errorClass,
                                Collection<Integer> expectedValues,
                                boolean allowIncompleteIfError) {
        Map<Integer, Integer> ac = new LinkedHashMap<>(), ex = new LinkedHashMap<>();
        subscriber.forEach(i     -> ac.put(i, ac.getOrDefault(i, 0)+1));
        expectedValues.forEach(i -> ex.put(i, ex.getOrDefault(i, 0)+1));
        if (errorClass != null) {
            if (!subscriber.hasError())
                fail("Expected "+errorClass);
            else
                assertEquals(errorClass, requireNonNull(subscriber.error()).getClass());
        } else if (subscriber.hasError()) {
            fail(subscriber.error());
        }
        if (errorClass != null && allowIncompleteIfError) {
            for (int i : ac.keySet())
                assertTrue(ex.containsKey(i), "Unexpected item "+i);
        } else {
            assertEquals(ex.keySet(), ac.keySet());
            for (Map.Entry<Integer, Integer> e : ex.entrySet()) {
                assertEquals(e.getValue(), ac.get(e.getKey()),
                             "count mismatch for item " + e.getKey());
            }
        }
    }

    private static final class TestException extends RuntimeException {
        public TestException(String message) { super(message); }
    }

    @Value
    private static class Source implements Publisher<Integer> {
        int begin, end, failBefore;

        @Override public void subscribe(Subscriber<? super Integer> s) {
            SourceSubscription subscription = new SourceSubscription(begin, end, failBefore, s);
            s.onSubscribe(subscription);
        }

        private final static class SourceSubscription implements Subscription {
            private final int end, failBefore;
            private final Subscriber<? super Integer> downstream;
            private boolean terminated = false;
            private long requestedEnd;
            private Thread requestingThread = null;
            private int i;

            public SourceSubscription(int begin, int end, int failBefore,
                                      Subscriber<? super Integer> downstream) {
                this.requestedEnd = this.i = begin;
                this.end = end;
                this.failBefore = failBefore;
                this.downstream = downstream;
            }

            @Override public void request(long n) {
                requestedEnd = Math.min(end, requestedEnd+n);
                if (requestingThread != null) {
                    assert requestingThread == Thread.currentThread();
                    return;
                }
                requestingThread = Thread.currentThread();
                try {
                    while (!terminated && i < requestedEnd) {
                        int item = i++;
                        if (item == failBefore) {
                            downstream.onError(new TestException("failBefore="+item));
                            terminated = true;
                        } else if (item < failBefore) {
                            downstream.onNext(item);
                        }
                    }
                    if (!terminated && i == end) {
                        downstream.onComplete();
                        terminated = true;
                    }
                } finally {
                    requestingThread = null;
                }
            }

            @Override public void cancel() {
                terminated = true;
            }
        }
    }

    @Value
    private static class AsyncSource implements Publisher<Integer> {
        int begin, end, failBefore;

        @Override public void subscribe(Subscriber<? super Integer> s) {
            SourceSubscription sub = new SourceSubscription(begin, end, failBefore, s);
            s.onSubscribe(sub);
        }

        private static final class SourceSubscription implements Subscription {
            private static final AtomicInteger nextId = new AtomicInteger(1);
            private final Semaphore requested = new Semaphore(0);
            private volatile boolean terminated;

            public SourceSubscription(int begin, int end, int failBefore,
                                      Subscriber<? super Integer> downstream) {
                Thread notifier = new Thread(() -> {
                    for (int i = begin; i < end; i++) {
                        if (i == failBefore) {
                            downstream.onError(new TestException("failBefore="+i));
                            terminated = true;
                            return;
                        }
                        requested.acquireUninterruptibly();
                        if (terminated)
                            return;
                        downstream.onNext(i);
                    }
                    downstream.onComplete();
                    terminated = true;
                });
                notifier.setName("Notifier-"+nextId.getAndIncrement());
                notifier.setDaemon(true);
                notifier.start();
            }

            @Override public void request(long n) {
                assert n >= 0;
                assert n < Integer.MAX_VALUE;
                requested.release((int) n);
            }

            @Override public void cancel() {
                if (!terminated) {
                    terminated = true;
                    requested.release();
                }
            }
        }
    }
    

    @Value
    private static class Range {
        int begin, end, failBefore;

        boolean hasError() { return failBefore < end; }

        Stream<Integer> stream() { return IntStream.range(begin, Math.min(end, failBefore)).boxed(); }

        Publisher<Integer> asPublisher(boolean async) {
            return async ? new AsyncSource(begin, end, failBefore)
                         : new Source(begin, end, failBefore);
        }
    }
    private static Range r(int begin, int end) { return new Range(begin, end, end+1); }
    private static Range r(int begin, int end, int failBefore) {
        return new Range(begin, end, failBefore);
    }

    @SuppressWarnings("unused") static Stream<Arguments> selfTest() {
        return Stream.of(false, true).flatMap(async -> Stream.of(
                arguments(async, 0, 0, 1),
                arguments(async, 0, 1, 2),
                arguments(async, 0, 2, 3),
                arguments(async, 0, 3, 4),
                arguments(async, 0, 64, 65),
                arguments(async, 0, 8192, 8193),
                arguments(async, 0, 1, 0),
                arguments(async, 0, 2, 1),
                arguments(async, 0, 3, 2),
                arguments(async, 0, 64, 32),
                arguments(async, 0, 8192, 4096)
        ));
    }

    @ParameterizedTest @MethodSource
    void selfTest(boolean async, int begin, int end, int failBefore) {
        for (int i = 0; i < N_ITERATIONS; i++) {
            Range range = r(begin, end, failBefore);
            List<Integer> ac = new ArrayList<>();
            IterableAdapter<Integer> subscriber = new IterableAdapter<>(range.asPublisher(async));
            subscriber.forEach(ac::add);
            if (failBefore < end)
                assertTrue(subscriber.hasError());
            assertEquals(range.stream().collect(toList()), ac);
        }
    }

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        List<List<Range>> base = new ArrayList<>(asList(
                // no sources
        /*  1 */emptyList(),
                // single empty source
        /*  2 */singletonList(r(0, 0)),
                // two empty sources
        /*  3 */asList(r(0, 0), r(1, 1)),
                // one empty failing source
        /*  4 */singletonList(r(0, 0, 0)),
                // two empty sources, one failing
        /*  5 */asList(r(0, 0, 0), r(0, 0)),
        /*  6 */asList(r(0, 0), r(0, 0, 0)),
                //single source, no failures
        /*  7 */singletonList(r(0, 1)),
        /*  8 */singletonList(r(0, QUEUE /2)),
        /*  9 */singletonList(r(0, QUEUE)),
        /* 10 */singletonList(r(0, QUEUE +1)),
        /* 11 */singletonList(r(0, QUEUE *4)),
                // empty first source, non-empty second with no errors
        /* 12 */asList(r(0, 0), r(0, 1)),
        /* 13 */asList(r(0, 0), r(0, QUEUE /2)),
        /* 14 */asList(r(0, 0), r(0, QUEUE)),
        /* 15 */asList(r(0, 0), r(0, QUEUE +1)),
        /* 16 */asList(r(0, 0), r(0, QUEUE *4)),
                // two sources, same size, no failures
        /* 17 */asList(r(0, 1),            r(0, 1)),
        /* 18 */asList(r(0, QUEUE /2), r(0, QUEUE /2)),
        /* 19 */asList(r(0, QUEUE),   r(0, QUEUE)),
        /* 20 */asList(r(0, QUEUE +1), r(0, QUEUE +1)),
        /* 21 */asList(r(0, QUEUE *4), r(0, QUEUE *4)),
                // three sources, same size, no failures
        /* 22 */asList(r(0, 1),            r(0, 1),            r(0, 1)),
        /* 23 */asList(r(0, QUEUE /2), r(0, QUEUE /2), r(0, QUEUE /2)),
        /* 24 */asList(r(0, QUEUE),   r(0, QUEUE),   r(0, QUEUE)),
        /* 25 */asList(r(0, QUEUE +1), r(0, QUEUE +1), r(0, QUEUE +1)),
        /* 26 */asList(r(0, QUEUE *4), r(0, QUEUE *4), r(0, QUEUE *4)),

                // single non-empty failing source
        /* 27 */singletonList(r(0, QUEUE /2, QUEUE /2-1)),
        /* 28 */singletonList(r(0, QUEUE, QUEUE -1)),
        /* 29 */singletonList(r(0, QUEUE +1, QUEUE)),
        /* 30 */singletonList(r(0, QUEUE *4, QUEUE *2)),
        /* 31 */singletonList(r(0, QUEUE *4, QUEUE *4-1)),

                //one long, the other failing midway
        /* 32 */asList(r(0, QUEUE *4), r(0, QUEUE*2, QUEUE)),
        /* 33 */asList(r(0, QUEUE *2, QUEUE), r(0, QUEUE *4)),

                //sequential ranges
        /* 34 */asList(r(0, QUEUE), r(QUEUE, QUEUE*2)),
        /* 35 */asList(r(0, QUEUE), r(QUEUE, QUEUE*2), r(QUEUE*2, QUEUE*3)),
        /* 36 */asList(r(0, QUEUE), r(QUEUE, QUEUE*2), r(QUEUE*2, QUEUE*3), r(QUEUE*3, QUEUE*4))
        ));

        //many sources failing midway
        for (int failBefore : asList(QUEUE /2 - 1, QUEUE /2, QUEUE *2)) {
            List<Range> concurrentFailing = new ArrayList<>();
            for (int i = 0; i < 16; i++)
                concurrentFailing.add(r(0, QUEUE *4, failBefore));
            base.add(concurrentFailing);
        }

        List<Arguments> list = new ArrayList<>();

        for (int concurrency : asList(1, -1, 2, 65536)) {
            for (Boolean ignoreUpstreamErrors : asList(false, true)) {
                for (Boolean subscribeEarly : asList(false, true)) {
                    for (Boolean asyncSource : asList(false, true)) {
                        for (List<Range> ranges : base) {
                            List<Publisher<Integer>> sources = ranges.stream()
                                    .map(r -> r.asPublisher(asyncSource)).collect(toList());
                            List<Integer> expected = ranges.stream().flatMap(Range::stream)
                                                           .collect(toList());
                            Class<? extends Throwable> error =
                                    ranges.stream()
                                    .anyMatch(Range::hasError) ? TestException.class : null;
                            int effConcurrency = concurrency < 0
                                               ? Math.max(1, sources.size()) : concurrency;
                            list.add(arguments(sources, ignoreUpstreamErrors,
                                               effConcurrency, subscribeEarly, error, expected));
                        }
                    }
                }
            }
        }
        return list.stream();
    }

    private static BoundedEventLoopPool pool;
    private static int poolThreads;

    @BeforeAll
    static void beforeAll() {
        poolThreads = Math.max(8, Runtime.getRuntime().availableProcessors());
        String poolName = MultiThreadedMergePublisherTest.class.getSimpleName() + "-pool";
        pool = new BoundedEventLoopPool(poolName, poolThreads);
    }

    @AfterAll
    static void afterAll() {
        try {
            Semaphore semaphore = new Semaphore(0);
            long start = System.nanoTime();
            for (int i = 0; i < poolThreads; i++)
                pool.chooseExecutor().execute(semaphore::release);
            semaphore.acquireUninterruptibly(poolThreads);
            double elapsedMs = (System.nanoTime() - start) / 1000000.0;
            assertTrue(elapsedMs < 200,
                    elapsedMs + "ms is too slow, possible leftover tasks");
        } finally {
            pool.close();
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 4})
    void testEmpty(int concurrency) throws ExecutionException {
        MultiThreadedMergePublisher<Object> pub = MultiThreadedMergePublisher.concurrent(concurrency)
                .pool(pool).name("empty-" + concurrency).build();
        AsyncTask<List<Object>> task = Async.async(() -> consume(pub));
        assertNull(task.orElse(null, 100, TimeUnit.MILLISECONDS));
        pub.markCompletable();
        assertEquals(Collections.emptyList(), task.get());
    }

    private <T> List<T> consume(Publisher<T> pub) {
        return consume(pub, null);
    }

    private <T> List<T> consume(Publisher<T> pub, @Nullable Throwable expectedError) {
        List<T> list = new ArrayList<>();
        IterableAdapter<T> a = new IterableAdapter<>(pub);
        a.forEach(list::add);
        assertEquals(expectedError, a.error());
        return list;
    }

    @Test
    void testSingleSource() {
        MultiThreadedMergePublisher<Object> pub = MultiThreadedMergePublisher.eager().pool(pool).name("test").build();
        pub.addPublisher(Flux.range(0, 3));
        pub.markCompletable();
        assertEquals(asList(0, 1, 2), consume(pub));
    }

    @Test
    void testTwoSourcesSerial() {
        MultiThreadedMergePublisher<Object> pub = MultiThreadedMergePublisher.eager().pool(pool).name("test").build();
        pub.addPublisher(Flux.range(0, 3));
        pub.addPublisher(Flux.range(3, 3));
        pub.markCompletable();
        assertEquals(asList(0, 1, 2, 3, 4, 5), consume(pub));
    }

    @Test
    void testSecondSourceFails() {
        MultiThreadedMergePublisher<Object> pub = MultiThreadedMergePublisher.eager().pool(pool).name("test").build();
        pub.addPublisher(Flux.range(0, 3));
        RuntimeException exception = new RuntimeException("err");
        pub.addPublisher(Mono.error(exception));
        pub.markCompletable();
        assertEquals(asList(0, 1, 2), consume(pub, exception));
    }

    @Test
    void testIgnoreFailure() {
        MultiThreadedMergePublisher<Object> pub = MultiThreadedMergePublisher.eager().pool(pool)
                .name("ignore-fail").ignoreUpstreamErrors(true).build();
        pub.addPublisher(Flux.range(0, 2));
        pub.addPublisher(Mono.error(new RuntimeException("ignored")));
        pub.addPublisher(Flux.range(2, 2));
        pub.markCompletable();
        assertEquals(asList(0, 1, 2, 3), consume(pub));
    }

    @Test
    void testConsumeBeforeAddingPublishers() throws ExecutionException {
        MultiThreadedMergePublisher<Integer> pub = MultiThreadedMergePublisher.eager().pool(pool).name("test").build();
        AsyncTask<List<Integer>> consumeTask = Async.async(() -> consume(pub));
        assertNull(consumeTask.orElse(null, 40, TimeUnit.MILLISECONDS));
        pub.addPublisher(Flux.range(0, 2));
        assertNull(consumeTask.orElse(null, 20, TimeUnit.MILLISECONDS));
        pub.addPublisher(Flux.range(2, 2));
        pub.addPublisher(Flux.range(4, 2));
        pub.markCompletable();
        assertEquals(asList(0, 1, 2, 3, 4, 5), consumeTask.get());
    }

    @ParameterizedTest @ValueSource(ints = {1, 4, 8, 16, 64})
    void testSerialSources(int sources) {
        int size = 16, repetitions = 32;
        for (int repetition = 0; repetition < repetitions; repetition++) {
            MultiThreadedMergePublisher<Integer> pub = MultiThreadedMergePublisher.eager().name("test-" + sources).build();
            for (int i = 0; i < sources; i++)
                pub.addPublisher(Flux.range(i*size, size));
            AsyncTask<List<Integer>> actual = Async.async(() -> consume(pub));
            for (int i = sources; i < 2*sources; i++)
                pub.addPublisher(Flux.range(i*size, size));
            pub.markCompletable();
            assertEquals(IntStream.range(0, size*2*sources).boxed().collect(toList()),
                    actual.fetch());
        }
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 5})
    void testTargetConcurrency(int sleepMs) throws InterruptedException, ExecutionException {
        int sourceSize = 32, sources = 32, repetitions = 8;
        for (int repetition = 0; repetition < repetitions; repetition++) {
            MultiThreadedMergePublisher<Integer> pub = MultiThreadedMergePublisher.concurrent(2)
                    .name("test-"+sleepMs+"-"+repetition+"").build();
            AsyncTask<List<Integer>> actual = Async.async(() -> consume(pub));
            for (int i = 0; i < sources; i++) {
                pub.addPublisher(Flux.range(i*sourceSize, sourceSize));
                if (sleepMs > 0) Thread.sleep(1);
            }
            pub.markCompletable();
            assertEquals(IntStream.range(0, sourceSize*sources).boxed().collect(toSet()),
                    new HashSet<>(actual.get()));
        }
    }

    @Test
    void testHugeTargetFewSources() {
        List<AsyncTask<?>> tasks = new ArrayList<>();
        for (int thread = 0; thread < N_THREADS; thread++) {
            tasks.add(Async.async(() -> {
                for (int i = 0; i < 16; i++) {
                    MultiThreadedMergePublisher<Integer> pub = MultiThreadedMergePublisher.concurrent(128000)
                            .name("test").build();
                    pub.addPublisher(Flux.range(0, 1));
                    pub.addPublisher(Flux.range(1, 2));
                    pub.markCompletable();
                    assertEquals(new HashSet<>(asList(0, 1, 2)), new HashSet<>(consume(pub)));
                }
            }));
        }
        for (AsyncTask<?> task : tasks) task.fetch();
    }

    @Test
    void testHugeTargetFewSourcesSerial() {
        MultiThreadedMergePublisher<Integer> pub = MultiThreadedMergePublisher.eager().name("test").build();
        pub.addPublisher(Flux.range(0, 1));
        pub.addPublisher(Flux.range(1, 2));
        pub.markCompletable();
        assertEquals(asList(0, 1, 2), consume(pub));
    }

    private static final AtomicInteger testMethodCall = new AtomicInteger(1);

    @ParameterizedTest @MethodSource
    void test(List<Publisher<Integer>> sources, boolean ignoreErrors,
              int concurrency, boolean subscribeEarly, @Nullable Class<? extends Throwable> error,
              Collection<Integer> expected) throws ExecutionException {
        String baseName = "test-" + testMethodCall.getAndIncrement();
        List<AsyncTask<?>> tasks = new ArrayList<>();
        for (int thread = 0; thread < N_THREADS; thread++) {
            String threadName = baseName+"[, ]thread="+thread;
            tasks.add(Async.asyncThrowing(() -> {
                for (int i = 0; i < N_ITERATIONS; i++) {
                    String iterationName = threadName + ", i=" + i + "]";
                    MultiThreadedMergePublisher<Integer> merger = MultiThreadedMergePublisher.builder()
                            .concurrency(concurrency).name(iterationName).pool(pool)
                            .ignoreUpstreamErrors(ignoreErrors).build();
                    IterableAdapter<Integer> subscriber = new IterableAdapter<>(merger, QUEUE);
                    if (subscribeEarly)
                        subscriber.start();
                    for (Publisher<Integer> source : sources)
                        merger.addPublisher(source);
                    merger.markCompletable();
                    boolean allowIncompleteIfError =
                            !ignoreErrors && error != null && sources.size() > 1;
                    Class<? extends Throwable> exError = ignoreErrors ? null : error;
                    assertExpected(subscriber, exError, expected, allowIncompleteIfError);
                }
            }));
        }
        for (AsyncTask<?> task : tasks) task.get();
    }
}