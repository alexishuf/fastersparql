package com.github.alexishuf.fastersparql.client.util.reactive;

import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CallbackPublisherTest {
    private static final int NO_ERROR = Integer.MAX_VALUE-1;
    private static final int SUBSCRIBER_QUEUE = 6;
    private static final int N_ITERATIONS = 2;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors()*4;

    private interface Callback {
        void onItem(int value);
        void done();
        void error(Throwable cause);
    }

    @SuppressWarnings("UnusedReturnValue")
    private static class Producer {
        private final Semaphore semaphore = new Semaphore(1);
        private final Thread thread;
        private boolean paused, stop;

        public Producer(int count, int errorAt, Throwable error, Callback callback) {
            this.thread = new Thread(() -> {
                for (int i = 0; !stop && i < count; i++) {
                    semaphore.acquireUninterruptibly();
                    semaphore.release();
                    callback.onItem(i);
                    Thread.yield();
                    if (i == errorAt)
                        callback.error(error);
                }
                callback.done();
            });
        }

        public Producer start() {
            thread.start();
            return this;
        }

        public Producer cancel() {
            stop = true;
            return resume();

        }

        public Producer pause() {
            if (!paused) {
                semaphore.acquireUninterruptibly();
                paused = true;
            }
            return this;
        }

        public Producer resume() {
            if (paused) {
                semaphore.release();
                paused = false;
            }
            return this;
        }

        public void sync() {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class TestPublisher extends CallbackPublisher<Integer> implements Callback{
        private static final AtomicInteger nextId = new AtomicInteger(1);
        @MonotonicNonNull Producer producer;

        public TestPublisher() {
            super("TestPublisher-"+nextId.getAndIncrement());
        }

        public void producer(Producer value) { producer = value; }

        /* --- --- --- CallbackPublisher methods --- --- --- */

        @Override protected void onRequest(long n) { producer.resume(); }
        @Override protected void onBackpressure()  { producer.pause(); }
        @Override protected void onCancel()        { producer.cancel(); }

        /* --- --- --- Callback methods --- --- --- */

        @Override public void onItem(int value)      { feed(value); }
        @Override public void done()                 { complete(null); }
        @Override public void error(Throwable cause) { complete(cause); }
    }

    private IterableAdapter<Integer> subscribe(TestPublisher pub) {
        //noinspection resource
        return new IterableAdapter<>(pub, 4).start();
    }

    static Stream<Arguments> data() {
        RuntimeException exception = new RuntimeException("on purpose");
        return Stream.of(
                arguments(0, NO_ERROR, null),
                arguments(1, NO_ERROR, null),
                arguments(2, NO_ERROR, null),

                arguments(SUBSCRIBER_QUEUE/2,   NO_ERROR, null),
                arguments(SUBSCRIBER_QUEUE,     NO_ERROR, null),
                arguments(SUBSCRIBER_QUEUE*4,   NO_ERROR, null),
                arguments(SUBSCRIBER_QUEUE*256, NO_ERROR, null),

                arguments(1, 0, exception),
                arguments(2, 1, exception),

                arguments(SUBSCRIBER_QUEUE, SUBSCRIBER_QUEUE/2, exception),
                arguments(SUBSCRIBER_QUEUE, Math.max(0, SUBSCRIBER_QUEUE/2 - 1), exception),
                arguments(SUBSCRIBER_QUEUE, Math.max(0, SUBSCRIBER_QUEUE/2 - 2), exception),

                arguments(SUBSCRIBER_QUEUE, SUBSCRIBER_QUEUE-1, exception),
                arguments(SUBSCRIBER_QUEUE, Math.max(0, SUBSCRIBER_QUEUE - 1), exception),
                arguments(SUBSCRIBER_QUEUE, Math.max(0, SUBSCRIBER_QUEUE - 2), exception),

                arguments(SUBSCRIBER_QUEUE*2, SUBSCRIBER_QUEUE, exception),
                arguments(SUBSCRIBER_QUEUE*2, Math.max(0, SUBSCRIBER_QUEUE - 1), exception),
                arguments(SUBSCRIBER_QUEUE*2, Math.max(0, SUBSCRIBER_QUEUE - 2), exception),

                arguments(SUBSCRIBER_QUEUE*8, SUBSCRIBER_QUEUE*4, exception)
        );
    }

    private void assertExpected(int count, int errorAt, Throwable cause,
                                IterableAdapter<Integer> subscriber) {
        assertTrue(errorAt < Integer.MAX_VALUE);
        ArrayList<Integer> actual = new ArrayList<>(), expected = new ArrayList<>();
        subscriber.forEach(actual::add);
        for (int i = 0; i < Math.min(count, errorAt+1); i++)
            expected.add(i);
        if (cause != null)
            assertEquals(cause, subscriber.error());
        else if (subscriber.hasError())
            fail(subscriber.error());
        assertEquals(expected, actual);
    }


    @ParameterizedTest @MethodSource("data")
    public void testSubscribeEarlyWaitSubscriber(int count, int errorAt, Throwable cause)
            throws ExecutionException {
        for (int i = 0; i < N_ITERATIONS; i++) {
            List<AsyncTask<?>> tasks = new ArrayList<>();
            for (int j = 0; j < N_THREADS; j++) {
                tasks.add(Async.async(() -> {
                    for (int k = 0; k < N_ITERATIONS; k++) {
                        TestPublisher pub = new TestPublisher();
                        Producer producer = new Producer(count, errorAt, cause, pub);
                        pub.producer(producer);
                        IterableAdapter<Integer> iterable = subscribe(pub);
                        producer.start();
                        assertExpected(count, errorAt, cause, iterable);
                        assertTimeout(Duration.ofMillis(100), () -> producer.cancel().sync());
                    }
                }));
            }
            for (AsyncTask<?> task : tasks)
                task.get();
        }
    }

    @ParameterizedTest @MethodSource("data")
    public void testSubscribeConcurrentlyWaitSubscriber(int count, int errorAt, Throwable cause)
            throws ExecutionException {
        for (int i = 0; i < N_ITERATIONS; i++) {
            List<AsyncTask<?>> tasks = new ArrayList<>();
            for (int j = 0; j < N_THREADS; j++) {
                tasks.add(Async.async(() -> {
                    for (int k = 0; k < N_ITERATIONS; k++) {
                        TestPublisher pub = new TestPublisher();
                        Producer producer = new Producer(count, errorAt, cause, pub);
                        pub.producer(producer);
                        producer.start();
                        IterableAdapter<Integer> iterable = subscribe(pub);
                        assertExpected(count, errorAt, cause, iterable);
                        assertTimeout(Duration.ofMillis(100), () -> producer.cancel().sync());
                    }
                }));
            }
            for (AsyncTask<?> task : tasks)
                task.get();
        }
    }

    @ParameterizedTest @MethodSource("data")
    public void testSubscribeLateWaitSubscriber(int count, int errorAt, Throwable cause)
            throws ExecutionException {
        List<AsyncTask<?>> tasks = new ArrayList<>();
        for (int j = 0; j < N_THREADS; j++) {
            tasks.add(Async.asyncThrowing(() -> {
                for (int k = 0; k < N_ITERATIONS; k++) {
                    TestPublisher pub = new TestPublisher();
                    Producer producer = new Producer(count, errorAt, cause, pub);
                    pub.producer(producer);
                    producer.start();
                    Thread.sleep(50);
                    IterableAdapter<Integer> iterable = subscribe(pub);
                    assertExpected(count, errorAt, cause, iterable);
                    assertTimeout(Duration.ofMillis(300), () -> producer.cancel().sync());
                }
            }));
        }
        for (AsyncTask<?> task : tasks)
            task.get();
    }

}