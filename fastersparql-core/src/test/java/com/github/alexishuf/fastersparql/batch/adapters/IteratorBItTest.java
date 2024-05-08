package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.intsBatch;
import static com.github.alexishuf.fastersparql.util.owned.Orphan.takeOwnership;
import static org.junit.jupiter.api.Assertions.*;

class IteratorBItTest extends AbstractBItTest {
    @Override protected List<? extends Scenario> scenarios() { return baseScenarios(); }

    @Override protected void run(Scenario s) {
        var list = IntStream.range(0, s.size()).boxed().toList();
        var it = ThrowingIterator.andThrow(list.iterator(), s.error);
        var bit = new IteratorBIt<>(it, TermBatchType.TERM, X);
        s.drainer.drainOrdered(bit, s.expectedInts(), s.error());
    }

    @Test void testMaxWait() throws Exception {
        var semaphore = new Semaphore(0);
        //noinspection resource
        var bit = new IteratorBIt<>(new Iterator<>() {
            private int next = 1;
            @Override public boolean hasNext() {
                semaphore.acquireUninterruptibly();
                semaphore.release(1);
                return next <= 3;
            }
            @Override public Integer next() { return next++; }
        }, TermBatchType.TERM, X);
        bit.minBatch(2).maxWait(Duration.ofMillis(50));
        try (var g = new Guard.ItGuard<>(this, bit);
             var ex1 = new Guard.BatchGuard<>(intsBatch(1), this);
             var ex23 = new Guard.BatchGuard<>(intsBatch(2, 3), this)
        ) {
            Semaphore started = new Semaphore(0);
            var batchFuture = new CompletableFuture<TermBatch>();
            Thread.ofVirtual().start(() -> {
                long start = System.nanoTime();
                started.release();
                var batch = g.nextBatch();
                if (System.nanoTime()-start < 40_000_000L)
                    batchFuture.completeExceptionally(new AssertionFailedError("did not honor maxWait"));
                else
                    batchFuture.complete(batch);
            });
            // release hasNext() at least 50ms after nextBatch() started
            started.acquireUninterruptibly();
            busySleepMillis(60);
            semaphore.release(1);

            // nextBatch must complete with a single item.
            // Although the second hasNext() call would return without blocking, there should be
            // no second call since maxWait has been elapsed
            assertEquals(ex1.get(), batchFuture.get());

            // from now on test we can consume the remainder
            assertTimeout(Duration.ofMillis(50), () -> {
                assertEquals(ex23.get(), bit.nextBatch(null));
                assertNull(bit.nextBatch(null));
            });
        }

    }

    public static void main(String[] ignored) throws IOException, ParseException {
        IteratorBItTest t = new IteratorBItTest();
//        try {
//            t.testMinWait();
//        } catch (Throwable ignored) {}
        try (var rec = new Recording(Configuration.getConfiguration("profile"))) {
            rec.setDumpOnExit(true);
            rec.setDestination(Path.of("/tmp/profile.jfr"));
            rec.start();
            for (int i = 0; i < 1; i++)
                t.testMinWait();
        }
    }

    @Test void testMinWait() {
        int delay = 100;
        long before = Timestamp.nanoTime();
        busySleepMillis(delay);
        assertTrue(Timestamp.nanoTime()-before/1_000_000L >= delay-1);
        @SuppressWarnings("resource")
        var bit = new IteratorBIt<>(new Iterator<>() {
            private int next = 1;
            @Override public boolean hasNext() {
                if (next > 4) return false;
                busySleepMillis(delay);
                return true;
            }
            @Override public Integer next() { return next++; }
        }, TermBatchType.TERM, X);
        bit.minBatch(2).minWait(Duration.ofMillis(2*delay + delay/2));

        List<TermBatch> batches = new ArrayList<>();
        try (var ex123 = new Guard.BatchGuard<>(intsBatch(1, 2, 3), this);
             var ex4   = new Guard.BatchGuard<>(intsBatch(4),       this)) {
            assertTimeout(Duration.ofMillis(delay*(3+1)),
                    () -> batches.add(takeOwnership(bit.nextBatch(null), this)));
            assertTimeout(Duration.ofMillis(delay*(1+1)),
                    () -> batches.add(takeOwnership(bit.nextBatch(null), this)));
            assertTimeout(Duration.ofMillis(delay),
                    () -> batches.add(takeOwnership(bit.nextBatch(null), this)));
            assertTimeout(Duration.ofMillis(delay),
                    () -> batches.add(takeOwnership(bit.nextBatch(null), this)));
            assertEquals(Arrays.asList(ex123.get(), ex4.get(), null, null),
                    batches);
        } finally {
            for (TermBatch b : batches)
                Batch.safeRecycle(b, this);
        }
    }
}