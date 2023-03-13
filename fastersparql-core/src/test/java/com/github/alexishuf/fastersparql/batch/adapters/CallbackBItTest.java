package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBufferedBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.NotRowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.VThreadTaskSet.repeatAndWait;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CallbackBItTest extends AbstractBItTest {
    private final AtomicInteger nextId = new AtomicInteger(1);

    @Override protected void run(Scenario genericScenario) {
        if (!(genericScenario instanceof BoundedScenario s))
            throw new IllegalArgumentException("Expected BoundedScenario");
        var cb = new SPSCBufferedBIt<>(NotRowType.INTEGER, Vars.EMPTY);
        cb.maxReadyBatches(s.maxReadyBatches()).maxReadyItems(s.maxReadyItems());
        cb.minBatch(s.minBatch()).maxBatch(s.maxBatch());
        int id = nextId.getAndIncrement();
        Thread.currentThread().setName("CallbackBItTest.run-"+id);
        Thread feeder = Thread.ofVirtual().name("feeder-"+id).start(() -> {
            for (int i = 0; i < s.size(); i++) cb.feed(i);
            cb.complete(s.error());
        });
        s.drainer().drainOrdered(cb, s.expected(), s.error());
        try {
            assertTrue(feeder.join(ofSeconds(2)));
        } catch (InterruptedException e) {
            fail(e);
        }
    }

    private static final class BoundedScenario extends Scenario {
        private final int maxReadyBatches, maxReadyItems;

        public BoundedScenario(Scenario base, int maxReadyBatches, int maxReadyItems) {
            super(base);
            this.maxReadyBatches = maxReadyBatches;
            this.maxReadyItems = maxReadyItems;
        }

        public int maxReadyBatches() { return maxReadyBatches; }
        public int maxReadyItems() { return maxReadyItems; }

        @Override public String toString() {
            return "BoundedScenario{"+"maxReadyBatches="+maxReadyBatches+", maxReadyElements="
                    +maxReadyItems+", size="+size+", minBatch="+minBatch+", maxBatch="+maxBatch
                    +", drainer="+drainer+", error="+error+'}';
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BoundedScenario r)) return false;
            if (!super.equals(o)) return false;
            return maxReadyBatches == r.maxReadyBatches && maxReadyItems == r.maxReadyItems;
        }

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), maxReadyBatches, maxReadyItems);
        }
    }

    protected List<BoundedScenario> scenarios() {
        var list = new ArrayList<BoundedScenario>();
        for (Scenario base : baseScenarios()) {
            for (List<Integer> maxReady : List.of(List.of(1, Integer.MAX_VALUE),
                    List.of(2, Integer.MAX_VALUE),
                    List.of(Integer.MAX_VALUE, base.maxBatch()),
                    List.of(Integer.MAX_VALUE, 3 * base.maxBatch()))) {
                list.add(new BoundedScenario(base, maxReady.get(0), maxReady.get(1)));
            }
        }
        return list;
    }

    @Test @Timeout(5)
    void testMergeBatches() throws Exception {
        try (var bit = new SPSCBufferedBIt<>(NotRowType.INTEGER, Vars.EMPTY)) {
            assertEquals(1, bit.minBatch());
            bit.maxBatch(3);
            bit.feed(0);
            assertEquals(new Batch<>(new Integer[]{0}, 1), bit.nextBatch());
            bit.feed(1);
            bit.feed(2);
            assertEquals(new Batch<>(new Integer[]{1, 2}, 2), bit.nextBatch());

            bit.feed(3);
            bit.feed(4);
            bit.feed(5);
            bit.feed(6);
            assertEquals(new Batch<>(new Integer[]{3, 4, 5}, 3), bit.nextBatch());
            assertEquals(new Batch<>(new Integer[]{6      }, 1), bit.nextBatch());

            CompletableFuture<Batch<Integer>> empty = new CompletableFuture<>();
            Thread thread = Thread.ofVirtual().start(() -> empty.complete(bit.nextBatch()));
            assertFalse(thread.join(ofMillis(100)));
            bit.complete(null);
            assertEquals(new Batch<>(Integer.class, 0), empty.get());
        }
    }

    @ParameterizedTest @MethodSource("timingReliableBatchGetters")
    void testMinWait(BatchGetter getter) {
        int delay = 20;
        try (var it = new SPSCBufferedBIt<>(NotRowType.INTEGER, Vars.EMPTY)) {
            it.minBatch(2).minWait(delay, TimeUnit.MILLISECONDS);

            long start = nanoTime();
            it.feed(1);
            it.feed(2);
            it.feed(3);
            List<Integer> batch = getter.getList(it);
            double ms = (nanoTime()-start)/1_000_000.0;
            assertTrue(Math.abs(ms-delay) < 10, "elapsed="+ms+" not in "+delay+"±10");
            assertEquals(List.of(1, 2, 3), batch);

            // do not wait if batch is last
            start = nanoTime();
            it.feed(4);
            it.complete(null);
            batch = getter.getList(it);
            ms = (nanoTime()-start)/1_000_000.0;
            assertEquals(List.of(4), batch);
            assertTrue(ms < 10, "too slow, ms="+ms);

            // do not wait after end
            start = nanoTime();
            batch = getter.getList(it);
            ms = (nanoTime()-start)/1_000_000.0;
            assertEquals(List.of(), batch);
            assertTrue(ms < 10, "too slow, ms="+ms);
        }
    }

    @ParameterizedTest @MethodSource("timingReliableBatchGetters")
    void testMaxWait(BatchGetter getter) throws Exception {
        int delay = 20, tolerance = 5;
        try (var it = new SPSCBufferedBIt<>(NotRowType.INTEGER, Vars.EMPTY)) {
            it.minBatch(2).maxWait(delay, TimeUnit.MILLISECONDS);
            List<List<Integer>> batches = new ArrayList<>();
            it.feed(1);
            Thread thread  = Thread.ofVirtual().start(() -> batches.add(getter.getList(it)));
            assertFalse(thread.join(ofMillis(tolerance)));
            busySleepMillis(delay);
            assertTrue(thread.join(ofMillis(tolerance)));

            it.feed(2);
            it.feed(3);
            assertTimeout(ofMillis(tolerance), () -> batches.add(getter.getList(it)));

            it.complete(null);
            assertTimeout(ofMillis(tolerance), () -> batches.add(getter.getList(it)));
            assertEquals(List.of(List.of(1), List.of(2, 3), List.of()), batches);
        }
    }

    private void testRaceFeedNextAndClose(int round, int minBatch, int waitBatches) {
        CompletableFuture<?> feed = new CompletableFuture<>(), drain = new CompletableFuture<>();
        var stop = new AtomicBoolean();
        var prematureExhaust = new AtomicBoolean(false);
        var suffix = format("{round=%d, min=%d, wait=%d}", round, minBatch, waitBatches);
        try (var it = new SPSCBufferedBIt<>(NotRowType.INTEGER, Vars.EMPTY)) {
            it.maxReadyItems(Math.max(65_536, 2*minBatch)).minBatch(minBatch);
            var batchDrained = new Semaphore(0);
            Thread.ofVirtual().name("Feeder"+suffix).start(() -> {
                try {
                    for (int i = 0; !stop.get(); i++)
                        it.feed(i);
                    feed.complete(null);
                } catch (BItCompletedException ignored) {
                    feed.complete(null);
                } catch (Throwable t) {
                    feed.completeExceptionally(t);
                }
            });
            Thread.ofVirtual().name("Drainer"+suffix).start(() -> {
                try {
                    for (var b = it.nextBatch(); b.size > 0; b = it.nextBatch(b))
                        batchDrained.release();
                    if (stop.compareAndSet(false, true))
                        prematureExhaust.set(true);
                    drain.complete(null);
                } catch (BItReadClosedException e) {
                    drain.complete(null);
                } catch (Throwable t) {
                    drain.completeExceptionally(t);
                } finally {
                    batchDrained.release(waitBatches);
                }
            });
            batchDrained.acquireUninterruptibly(waitBatches);
            stop.set(true);
        } // it.close()

        try {
            feed.get();
            drain.get();
        } catch (ExecutionException|InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertFalse(prematureExhaust.get());
    }

    static Stream<Arguments> testRaceFeedNextAndClose() {
        List<Arguments> list = new ArrayList<>();
        for (int minBatch : List.of(1, 2, 3, 4, 16, 128)) {
            for (int wait : List.of(0, 1, 2, 4))  list.add(arguments(minBatch, wait));
        }
        for (int minBatch : List.of(1024, 65_536)) {
            for (int wait : List.of(0, 1))  list.add(arguments(minBatch, wait));
        }
        return list.stream();
    }

    /** Concurrent calls to feed(), nextBatch() and close() */
    @ParameterizedTest @MethodSource
    void testRaceFeedNextAndClose(int minBatch, int waitBatches) throws Exception {
        int multiplier =  minBatch*waitBatches <= 128 ? 10 : 2;
        int rounds = multiplier*Runtime.getRuntime().availableProcessors();
        String name = ".testRaceFeedNextAndClose("+minBatch+", "+waitBatches+")";
        try {
            testRaceFeedNextAndClose(-1, minBatch, waitBatches);
            repeatAndWait(name, rounds, (Consumer<Integer>) round
                    -> testRaceFeedNextAndClose(round, minBatch, waitBatches));
        } finally {
            System.gc();
        }
    }
}