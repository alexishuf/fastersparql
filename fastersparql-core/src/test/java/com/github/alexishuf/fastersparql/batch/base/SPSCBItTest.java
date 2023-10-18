package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.*;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static java.lang.System.nanoTime;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Arrays.stream;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class SPSCBItTest extends CallbackBItTest {
    private static final int maxItems = 8;
    private static final Logger log = LoggerFactory.getLogger(SPSCBItTest.class);
    private final AtomicInteger nextId = new AtomicInteger(1);

    @Override protected CallbackBIt<TermBatch> create(int capacity) {
        return new SPSCBIt<>(TERM, Vars.of("x"), capacity);
    }

    @Override protected void run(Scenario genericScenario) {
        if (!(genericScenario instanceof BoundedScenario s))
            throw new IllegalArgumentException("Expected BoundedScenario");
         var cb = new SPSCBIt<>(TERM, Vars.of("x"), s.maxReadyItems());
        cb.maxReadyItems(s.maxReadyItems());
        cb.minBatch(s.minBatch()).maxBatch(s.maxBatch());
        int id = nextId.getAndIncrement();
        String oldName = Thread.currentThread().getName();
        Thread.currentThread().setName("CallbackBItTest.run-"+id);
        Thread feeder = Thread.ofVirtual().name("feeder-"+id).start(() -> {
            for (int i = 0; i < s.size(); i++) {
                try {
                    cb.offer(intsBatch(i));
                } catch (TerminatedException|CancelledException ignored) {}
            }
            cb.complete(s.error());
        });
        s.drainer().drainOrdered(cb, s.expectedInts(), s.error());
        try {
            assertTrue(feeder.join(ofSeconds(2)));
        } catch (InterruptedException e) {
            fail(e);
        } finally {
            Thread.currentThread().setName(oldName);
        }
    }

    protected static final class BoundedScenario extends Scenario {
        private final int maxReadyItems;

        public BoundedScenario(Scenario base, int maxReadyItems) {
            super(base);
            this.maxReadyItems = maxReadyItems;
        }

        public int maxReadyItems() { return maxReadyItems; }

        @Override public String toString() {
            return "BoundedScenario{maxReadyElements=" +maxReadyItems+", size="+size+", " +
                    "minBatch="+minBatch+", maxBatch="+maxBatch +", drainer="+drainer+
                    ", error="+error+'}';
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BoundedScenario r)) return false;
            if (!super.equals(o)) return false;
            return maxReadyItems == r.maxReadyItems;
        }

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), maxReadyItems);
        }
    }

    protected List<BoundedScenario> scenarios() {
        var list = new ArrayList<BoundedScenario>();
        for (Scenario base : baseScenarios()) {
            for (int maxReady : List.of(Integer.MAX_VALUE, 8, 2, base.maxBatch())) {
                list.add(new BoundedScenario(base, maxReady));
            }
        }
        return list;
    }


    @Test void testDoNotOfferToFillingWhenThereIsExcessCapacity() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 8)) {
            TermBatch b1 = tightIntsBatch(1, 2).withCapacity(3);
            TermBatch b2 = tightIntsBatch(3, 4);
            assertTrue(b1.rowsCapacity() >= 5);

            assertNull(assertDoesNotThrow(() -> it.offer(b1)));
            assertNull(assertDoesNotThrow(() -> it.offer(b2))); // do not offer() to b1, queue is mostly empty
        }
    }

    @Test void testOfferToFilling() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), maxItems)) {
            TermBatch b1 = tightIntsBatch(1), b2_ = tightIntsBatch(2, 3), b3 = tightIntsBatch(4, 5);
            assertEquals(2, b2_.rowsCapacity());
            var b2 = b2_.withCapacity(2);

            assertNull(assertDoesNotThrow(() -> it.offer(b1)));
            assertNull(assertDoesNotThrow(() -> it.offer(b2)));
            var b3_ = b3;
            assertSame(b3, assertDoesNotThrow(() -> it.offer(b3_))); // rows copied to b2, we own b3
            b3.clear();
            b3 = b3.putRow(Term.termList("23")); // invalidate b3

            assertSame(b1, it.nextBatch(b3));
            assertEquals(tightIntsBatch(1), b1);

            assertSame(b2, it.nextBatch(null));
            assertEquals(intsBatch(2, 3, 4, 5), b2);

            assertEquals(BIt.State.ACTIVE, it.state());
            it.complete(null);
            assertEquals(BIt.State.COMPLETED, it.state());

            assertNull(it.nextBatch(null));
        }
    }

    @Test
    void testOfferToUnpublishedRejected() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 4)) {
            TermBatch fst = tightIntsBatch(1, 2), snd = tightIntsBatch(3);
            assertEquals(2, fst.rowsCapacity());
            assertNull(assertDoesNotThrow(() -> it.offer(fst)));
            assertNull(assertDoesNotThrow(() -> it.offer(snd))); // bcs fst.offer(snd) == false
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
            assertNull(assertDoesNotThrow(() -> it.offer(b1)));
            it.complete(ex);
            assertSame(b1, it.nextBatch(null));
            assertThrows(TerminatedException.class, () -> it.offer(b1));

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

    @Test void testMaxReadyItems() throws InterruptedException, ExecutionException {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 2)) {
            TermBatch b1 = intsBatch(1), b2 = intsBatch(2, 3);
            TermBatch b3 = intsBatch(4, 5), b4 = intsBatch(5, 6);
            assertTimeout(Duration.ofMillis(5), () -> {
                assertNull(it.offer(b1));
                assertNull(it.offer(b2));
            });
            // start a offer(), which will block
            CompletableFuture<TermBatch> f3 = new CompletableFuture<>();
            Thread.startVirtualThread(
                    () -> f3.complete(assertDoesNotThrow(() -> it.offer(b3))));
            assertThrows(TimeoutException.class, () -> f3.get(5, MILLISECONDS));

            // consuming will unblock offer()
            assertSame(b1, it.nextBatch(null));
            assertNull(f3.get()); // offer() was unblocked

            // copy() must block() since b3 on filling has 2 items
            Thread t4 = Thread.startVirtualThread(
                    () -> assertDoesNotThrow(() -> it.copy(b4)));
            assertFalse(t4.join(ofMillis(5)));

            // consuming will unblock() copy
            assertSame(b2, it.nextBatch(null));
            assertTrue(t4.join(ofMillis(5)));

            // offer() and copy() are visible with order retained
            assertSame(b3, it.nextBatch(null));
            assertEquals(b4, it.nextBatch(null));
        }
    }

    @Test void testRecyclingRace() throws Exception {
        int threads = Math.max(2, Runtime.getRuntime().availableProcessors());
        int chunk = 1<<16, queueCap = 4;
        TermBatch[] batches   = new TermBatch[threads*chunk];
        TermBatch[] stolen    = new TermBatch[threads*chunk];
        int[] stolenCount     = new int[batches.length];
        for (int i = 0, n = chunk*threads; i < n; i++)
            batches[i] = IntsBatch.fill(new TermBatch(new Term[1], 0, 1), i+1);
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
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), maxItems)) {
            it.minBatch(2).minWait(delay, MILLISECONDS);

            TermBatch b1 = intsBatch(1); // before nanoTime() to avoid measuring class init
            long start = nanoTime();
            assertDoesNotThrow(() -> it.offer(b1));
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
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), maxItems)) {
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
}