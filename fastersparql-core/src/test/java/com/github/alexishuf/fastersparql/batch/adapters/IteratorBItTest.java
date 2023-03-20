package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.intsBatch;
import static org.junit.jupiter.api.Assertions.*;

class IteratorBItTest extends AbstractBItTest {
    private final static Vars X = Vars.of("x");

    @Override protected List<? extends Scenario> scenarios() { return baseScenarios(); }

    @Override protected void run(Scenario s) {
        var list = IntStream.range(0, s.size()).boxed().toList();
        var it = ThrowingIterator.andThrow(list.iterator(), s.error);
        var bit = new IteratorBIt<>(it, Batch.TERM, X);
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
        }, Batch.TERM, X);
        bit.minBatch(2).maxWait(Duration.ofMillis(50));

        Semaphore started = new Semaphore(0);
        var batchFuture = new CompletableFuture<TermBatch>();
        Thread.ofVirtual().start(() -> {
            long start = System.nanoTime();
            started.release();
            TermBatch batch = bit.nextBatch(null);
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
        assertEquals(intsBatch(1), batchFuture.get());

        // from now on test we can consume the remainder
        assertTimeout(Duration.ofMillis(50), () -> {
            assertEquals(intsBatch(2, 3), bit.nextBatch(null));
            assertNull(bit.nextBatch(null));
        });
    }

    @Test void testMinWait() {
        int delay = 10;
        @SuppressWarnings("resource")
        var bit = new IteratorBIt<>(new Iterator<>() {
            private int next = 1;
            @Override public boolean hasNext() {
                if (next > 4) return false;
                busySleepMillis(delay);
                return true;
            }
            @Override public Integer next() { return next++; }
        }, Batch.TERM, X);
        bit.minBatch(2).minWait(Duration.ofMillis(2*delay + delay/2));

        List<TermBatch> batches = new ArrayList<>();
        assertTimeout(Duration.ofMillis(delay*(3+1)), () -> batches.add(bit.nextBatch(null)));
        assertTimeout(Duration.ofMillis(delay*(1+1)), () -> batches.add(bit.nextBatch(null)));
        assertTimeout(Duration.ofMillis(delay), () -> batches.add(bit.nextBatch(null)));
        assertTimeout(Duration.ofMillis(delay), () -> batches.add(bit.nextBatch(null)));
        assertEquals(Arrays.asList(intsBatch(1, 2, 3), intsBatch(4), null, null),
                     batches);
    }
}