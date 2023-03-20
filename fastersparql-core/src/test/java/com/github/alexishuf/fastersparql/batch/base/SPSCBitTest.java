package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxBatches;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.*;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static java.lang.System.nanoTime;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.stream;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class SPSCBitTest extends CallbackBItTest {
    private static final Logger log = LoggerFactory.getLogger(SPSCBitTest.class);
    private final AtomicInteger nextId = new AtomicInteger(1);

    @Override protected CallbackBIt<TermBatch> create(int capacity) {
        return new SPSCBIt<>(TERM, Vars.of("x"), capacity);
    }

    @Override protected void run(Scenario genericScenario) {
        if (!(genericScenario instanceof BoundedScenario s))
            throw new IllegalArgumentException("Expected BoundedScenario");
         var cb = new SPSCBIt<>(TERM, Vars.of("x"), s.maxReadyBatches());
        cb.maxReadyItems(s.maxReadyItems());
        cb.minBatch(s.minBatch()).maxBatch(s.maxBatch());
        int id = nextId.getAndIncrement();
        Thread.currentThread().setName("CallbackBItTest.run-"+id);
        Thread feeder = Thread.ofVirtual().name("feeder-"+id).start(() -> {
            for (int i = 0; i < s.size(); i++) cb.offer(intsBatch(i));
            cb.complete(s.error());
        });
        s.drainer().drainOrdered(cb, s.expectedInts(), s.error());
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
            for (List<Integer> maxReady : List.of(
                    List.of(2, Integer.MAX_VALUE),
                    List.of(2, 8),
                    List.of(2, 2),
                    List.of(8, Integer.MAX_VALUE),
                    List.of(8, 2),
                    List.of(Integer.MAX_VALUE, base.maxBatch()))
            ) {
                list.add(new BoundedScenario(base, maxReady.get(0), maxReady.get(1)));
            }
        }
        return list;
    }


    @Test void testDoNotOfferToFillingWhenThereIsExcessCapacity() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 8)) {
            TermBatch b1 = tightIntsBatch(1, 2), b2 = tightIntsBatch(3, 4);
            b1.reserve(3, 0);
            assertEquals(5, b1.rowsCapacity());

            assertNull(it.offer(b1));
            assertNull(it.offer(b2)); // do not offer() to b1, queue is mostly empty
        }
    }

    @Test void testOfferToFilling() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 2)) {
            TermBatch b1 = tightIntsBatch(1), b2 = tightIntsBatch(2, 3), b3 = tightIntsBatch(4, 5);
            assertEquals(2, b2.rowsCapacity());
            b2.reserve(2, 0);

            assertNull(it.offer(b1));
            assertNull(it.offer(b2));
            assertSame(b3, it.offer(b3)); // rows copied to b2, we own b3
            b3.clear(); b3.putRow(Term.termList("23")); // invalidate b3

            assertSame(b1, it.nextBatch(b3));
            assertEquals(tightIntsBatch(1), b1);

            assertSame(b2, it.nextBatch(null));
            assertEquals(intsBatch(2, 3, 4, 5), b2);

            assertFalse(it.isCompleted());
            assertFalse(it.isFailed());
            it.complete(null);
            assertTrue(it.isCompleted());
            assertFalse(it.isFailed());

            assertNull(it.nextBatch(null));
        }
    }

    @Test
    void testOfferToUnpublishedRejected() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 4)) {
            TermBatch fst = tightIntsBatch(1, 2), snd = tightIntsBatch(3);
            assertEquals(2, fst.rowsCapacity());
            assertNull(it.offer(fst));
            assertNull(it.offer(snd)); // bcs fst.offer(snd) == false
            assertSame(fst, it.nextBatch(null));
            assertSame(snd, it.nextBatch(intsBatch(23)));
            it.complete(null);
            assertNull(it.nextBatch(intsBatch(27)));
            assertNull(it.nextBatch(null));
        }
    }

    @Test
    void testCompleteWithError() {
        RuntimeException ex = new RuntimeException("test");
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 4)) {
            TermBatch b1 = intsBatch(1);
            assertNull(it.offer(b1));
            it.complete(ex);
            assertSame(b1, it.nextBatch(null));
            assertThrows(BItCompletedException.class, () -> it.offer(b1));

            try {
                it.nextBatch(null);
                fail("Expected exception");
            } catch (BItReadFailedException e) {
                assertSame(ex, e.getCause());
                assertSame(ex, e.rootCause());
            } catch (Throwable e) {
                fail("Expected BItReadFailedException", e);
            }
        }
    }

    @Test void testRingBuffer() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 2)) {
            it.maxBatch(3);
            TermBatch b1 = tightIntsBatch(1), b2 = tightIntsBatch(2);
            TermBatch b3 = tightIntsBatch(3), b4 = tightIntsBatch(4);
            TermBatch b5 = tightIntsBatch(5), b6 = tightIntsBatch(6);
            assertEquals(1, b1.rowsCapacity());
            assertEquals(1, b2.rowsCapacity());
            assertEquals(1, b3.rowsCapacity());
            assertEquals(1, b4.rowsCapacity());
            assertNull(it.offer(b1)); // after: [[1]], items=0, wIdx=0, batches=0
            assertNull(it.offer(b2)); // after: [[1], [2]], items=1, wIdx=1, batches=1
            assertSame(b3, it.offer(b3)); // after: [[1], [2, 3]], items=1, wIdx=0, batches=1
            assertSame(b4, it.offer(b4)); // after: [[1], [2, 3, 4]], items=1, wIdx=0, batches=1

            // next offer will block due to maxBatch(3)
            var f5 = new CompletableFuture<TermBatch>();
            Thread.startVirtualThread(() -> f5.complete(it.offer(b5)));
            assertThrows(TimeoutException.class, () -> f5.get(5, MILLISECONDS));

            assertSame(b1, it.nextBatch(null)); // unblocks offer(b5)
            assertTimeout(ofMillis(20), () -> assertNull(f5.get()));
            // [[5], [2, 3, 4]], items=3, wIdx=0, batches=1
            assertSame(b6, it.offer(b6)); // after: [[5, 6], [2, 3, 4]], items=3, wIdx=0, batches=1

            assertSame(b2, it.nextBatch(null));
            assertEquals(intsBatch(2, 3, 4), b2);

            assertSame(b5, it.nextBatch(null));
            assertEquals(intsBatch(5, 6), b5);

            it.complete(null);
            assertNull(it.nextBatch(null));
        }
    }

    @Test void testMaxReadyItems() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 4).maxReadyItems(2)) {
            TermBatch b1 = tightIntsBatch(1, 2), b2 = tightIntsBatch(3);
            b1.reserve(1, 0);
            assertNull(it.offer(b1));

            CompletableFuture<TermBatch> f2 = new CompletableFuture<>();
            Thread.startVirtualThread(() -> f2.complete(it.offer(b2)));
            assertThrows(TimeoutException.class, () -> f2.get(5, MILLISECONDS));

            assertSame(b1, it.nextBatch(null));
            assertTimeout(ofMillis(20), () -> f2.get());
            assertSame(b2, it.nextBatch(b1));

            TermBatch b3 = tightIntsBatch(4, 5, 6);
            assertSame(b1, it.offer(b3)); // violates maxReadyItems bcs queue is empty

            TermBatch b4 = tightIntsBatch(4, 5, 6);
            CompletableFuture<TermBatch> f4 = new CompletableFuture<>();
            Thread.startVirtualThread(() -> f4.complete(it.offer(b4)));
            assertThrows(TimeoutException.class, () -> f4.get(5, MILLISECONDS));

            assertSame(b3, it.nextBatch(b2)); // allows offer(b4) to complete

            assertTimeout(ofMillis(20), () -> assertSame(b2, f4.get()));
            assertSame(b4, it.nextBatch(b3));
        }
    }

    @Test void testDirectRecycling() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 4)) {
            TermBatch b1 = intsBatch(1), b2 = intsBatch(2), b3 = intsBatch(3);
            TermBatch b4 = intsBatch(4), b5 = intsBatch(5), b6 = intsBatch(6);

            assertNull(it.recycle(b1));
            assertNull(it.recycle(b2));
            assertNull(it.recycle(b3));
            assertNull(it.recycle(b4));
            assertNull(it.recycle(b5)); // sent to AbstractBit.recycled
            assertSame(b6, it.recycle(b6)); // rejected

            assertSame(b1, it.stealRecycled());
            assertSame(b2, it.stealRecycled());
            assertSame(b3, it.stealRecycled());
            assertSame(b4, it.stealRecycled());
            assertSame(b5, it.stealRecycled());
            assertNull(it.stealRecycled());
        }
    }

    @Test void testRecyclingRace() throws Exception {
        int threads = Math.max(2, Runtime.getRuntime().availableProcessors());
        int chunk = 1<<16, queueCap = 4;
        TermBatch[] batches   = new TermBatch[threads*chunk];
        TermBatch[] stolen    = new TermBatch[threads*chunk];
        int[] stolenCount     = new int[batches.length];
        for (int i = 0, n = chunk*threads; i < n; i++)
            IntsBatch.fill(batches[i] = new TermBatch(1, 1), i+1);
        try (var tasks = new TestTaskSet("testRecyclingRace", newFixedThreadPool(threads));
             var it = new SPSCBIt<>(TERM, X, queueCap)) {
            for (int round = 0, rounds = 20; round < rounds; round++) {
                log.info("start round {}/{}", round, rounds);
                Arrays.fill(stolen, null);
                for (int threadIdx = 0; threadIdx < threads/2; threadIdx++) {
                    final int finalThreadIdx = threadIdx;
                    tasks.add(() -> {
                        Thread.currentThread().setName("producer-"+finalThreadIdx);
                        for (int i = finalThreadIdx*chunk, end = i+chunk; i < end; i++)
                            batches[i] = it.recycle(batches[i]);
                    });
                    tasks.add(() -> {
                        Thread.currentThread().setName("consumer-"+finalThreadIdx);
                        for (int i = finalThreadIdx*chunk, end = i+chunk; i < end; i++) {
                            stolen[i] = it.stealRecycled();
                        }
                    });
                }
                tasks.await(); // wait producers and consumers

                // check minimal number of recycled batches
                long recycled = stream(batches).filter(Objects::isNull).count();
                assertTrue(recycled >= queueCap, "number of recycled batches ("+recycled+") < queueCap ("+queueCap+")");

                // check for duplicate stealRecycled()
                Arrays.fill(stolenCount, 0);
                for (TermBatch b : stolen) {
                    if (b == null) continue;
                    assertEquals(1, b.rows);
                    ++stolenCount[IntsBatch.parse(b.get(0, 0))];
                }
                var stolenTwice = Arrays.toString(stream(stolenCount).filter(i -> i > 1).toArray());
                assertEquals("[]", stolenTwice, "batches stolen more than once: race");
            }
        }
    }


    @Test void testMinWait() {
        int delay = 20;
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), queueMaxBatches())) {
            it.minBatch(2).minWait(delay, MILLISECONDS);

            TermBatch b1 = intsBatch(1); // before nanoTime() to avoid measuring class init
            long start = nanoTime();
            it.offer(b1);
            IntsBatch.offerAndInvalidate(it, intsBatch(2));
            IntsBatch.offerAndInvalidate(it, intsBatch(3));
            TermBatch batch = it.nextBatch(null);
            double ms = (nanoTime()-start)/1_000_000.0;
            assertTrue(Math.abs(ms-delay) < 10, "elapsed="+ms+" not in "+delay+"±10");
            assertEquals(intsBatch(1, 2, 3), batch);

            // do not wait if batch is last
            start = nanoTime();
            IntsBatch.offerAndInvalidate(it, intsBatch(4));
            it.complete(null);
            batch = it.nextBatch(null);
            ms = (nanoTime()-start)/1_000_000.0;
            assertEquals(intsBatch(4), batch);
            assertTrue(ms < 10, "too slow, ms="+ms);

            // do not wait after end
            start = nanoTime();
            batch = it.nextBatch(null);
            ms = (nanoTime()-start)/1_000_000.0;
            assertNull(batch);
            assertTrue(ms < 10, "too slow, ms="+ms);
        }
    }

    @Test void testMaxWait() throws Exception {
        int delay = 20, tolerance = 5;
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), queueMaxBatches())) {
            it.minBatch(2).maxWait(delay, MILLISECONDS);
            List<TermBatch> batches = Collections.synchronizedList(new ArrayList<>());
            IntsBatch.offerAndInvalidate(it, intsBatch(1));
            Thread thread  = Thread.ofVirtual().start(() -> batches.add(it.nextBatch(null)));
            assertFalse(thread.join(ofMillis(tolerance)));
            busySleepMillis(delay);
            assertTrue(thread.join(ofMillis(tolerance)));

            IntsBatch.offerAndInvalidate(it, intsBatch(2));
            IntsBatch.offerAndInvalidate(it, intsBatch(3));
            assertTimeout(ofMillis(tolerance), () -> batches.add(it.nextBatch(null)));

            it.complete(null);
            assertTimeout(ofMillis(tolerance), () -> batches.add(it.nextBatch(null)));
            assertEquals(intsBatch(1), batches.get(0));
            assertEquals(intsBatch(2, 3), batches.get(1));
            assertNull(batches.get(2));
            assertEquals(3, batches.size());
        }
    }

    public static void main(String[] args) {
        var test = new SPSCBitTest();
        try {
            long start = nanoTime();
            while (nanoTime()-start < 10_000_000_000L)
                test.testRaceFeedNextAndClose(2, 4);
        } catch (Throwable ex) {
            throw ex instanceof RuntimeException re ? re : new RuntimeException(ex);
        }
    }

}