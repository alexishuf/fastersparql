package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.NotRowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;

class IteratorBItTest extends AbstractBItTest {

    @Override protected List<? extends Scenario> scenarios() { return baseScenarios(); }

    @Override protected void run(Scenario s) {
        var list = IntStream.range(0, s.size()).boxed().toList();
        var it = ThrowingIterator.andThrow(list.iterator(), s.error);
        var bit = new IteratorBIt<>(it, NotRowType.INTEGER, Vars.EMPTY);
        s.drainer.drainOrdered(bit, s.expected(), s.error());
    }

    @ParameterizedTest @MethodSource("batchGetters")
    void testMaxWait(BatchGetter getter) throws Exception {
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
        }, NotRowType.INTEGER, Vars.EMPTY);
        bit.minBatch(2).maxWait(Duration.ofMillis(50));

        Semaphore started = new Semaphore(0);
        var batchFuture = new CompletableFuture<Batch<Integer>>();
        Thread.ofVirtual().start(() -> {
            long start = System.nanoTime();
            started.release();
            Batch<Integer> batch = getter.getBatch(bit);
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
        assertEquals(new Batch<>(new Integer[] {1}, 1), batchFuture.get());

        // from now on test we can consume the remainder
        assertTimeout(Duration.ofMillis(50), () -> {
            assertEquals(new Batch<>(new Integer[]{2, 3}, 2), getter.getBatch(bit));
            assertEquals(new Batch<>(Integer.class, 0), getter.getBatch(bit));
        });
    }

    @ParameterizedTest @MethodSource("batchGetters")
    void testMinWait(BatchGetter getter) {
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
        }, NotRowType.INTEGER, Vars.EMPTY);
        bit.minBatch(2).minWait(Duration.ofMillis(2*delay + delay/2));

        List<List<Integer>> batches = new ArrayList<>();
        assertTimeout(Duration.ofMillis(delay*(3+1)), () -> batches.add(getter.getList(bit)));
        assertTimeout(Duration.ofMillis(delay*(1+1)), () -> batches.add(getter.getList(bit)));
        assertTimeout(Duration.ofMillis(delay), () -> batches.add(getter.getList(bit)));
        assertTimeout(Duration.ofMillis(delay), () -> batches.add(getter.getList(bit)));
        assertEquals(List.of(List.of(1, 2, 3), List.of(4), List.of(), List.of()), batches);
    }
}