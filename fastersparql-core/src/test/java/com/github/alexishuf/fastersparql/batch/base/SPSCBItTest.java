package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.intsBatch;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static java.lang.System.nanoTime;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;

class SPSCBItTest extends CallbackBItTest {
    private static final int maxItems = 8;
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
            var b1 = intsBatch(1, 2);
            var b2 = intsBatch(3, 4);
            assertDoesNotThrow(() -> it.offer(b1));
            assertDoesNotThrow(() -> it.offer(b2)); // do not offer() to b1, queue is mostly empty
        }
    }

    @Test void testOfferToFilling() {
        var ex1 = intsBatch(1).takeOwnership(this);
        var ex23456 = intsBatch(2, 3, 4, 5).takeOwnership(this);
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), maxItems);
             var b1Guard = new Guard.BatchGuard<TermBatch>(this);
             var b2Guard = new Guard.BatchGuard<TermBatch>(this);
             var b3Guard = new Guard.BatchGuard<>(intsBatch(4, 5), this)) {
            var b1 = b1Guard.set(intsBatch(1));
            var b2 = b2Guard.set(intsBatch(2, 3));

            assertDoesNotThrow(() -> it.offer(b1Guard.take()));
            assertDoesNotThrow(() -> it.offer(b2Guard.take()));
            assertDoesNotThrow(() -> it.offer(b3Guard.take())); // b2.append(b3), we lost ownership

            var offer = TERM.create(1).takeOwnership(this);
            assertSame(b1, b1Guard.set(it.nextBatch(offer.releaseOwnership(this))));
            assertEquals(ex1, b1);
            assertFalse(offer.isAliveAndMarking());

            assertSame(b2, b2Guard.set(it.nextBatch(null)));
            assertEquals(ex23456, b2);

            assertEquals(BIt.State.ACTIVE, it.state());
            it.complete(null);
            assertEquals(BIt.State.COMPLETED, it.state());

            assertNull(it.nextBatch(null));
        } finally {
            ex1.recycle(this);
            ex23456.recycle(this);
        }
    }

    @Test
    void testOfferToUnpublishedRejected() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 4);
             var fstG = new Guard.BatchGuard<TermBatch>(this);
             var sndG = new Guard.BatchGuard<TermBatch>(this)) {
            TermBatch fst = fstG.set(intsBatch(1, 2)), snd = sndG.set(intsBatch(3));
            assertDoesNotThrow(() -> it.offer(fstG.take()));
            assertDoesNotThrow(() -> it.offer(sndG.take())); // bcs fst.offer(snd) == false
            assertSame(fst, fstG.set(it.nextBatch(null)));

            Orphan<TermBatch> offer = intsBatch(23);
            assertSame(snd, sndG.set(it.nextBatch(offer)));
            assertFalse(((TermBatch)offer).isAliveAndMarking());

            it.complete(null);
            assertNull(it.nextBatch(offer = intsBatch(27)));
            assertFalse(((TermBatch)offer).isAliveAndMarking());
            assertNull(it.nextBatch(null));
        }
    }

    @Test
    void testCompleteWithError() {
        RuntimeException ex = new RuntimeException("test");
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 4)) {
            var b1 = intsBatch(1);
            assertDoesNotThrow(() -> it.offer(b1));
            it.complete(ex);
            assertSame(b1, it.nextBatch(null));
            assertThrows(TerminatedException.class, () -> it.offer(b1));
            assertFalse(((Batch<?>)b1).isAliveAndMarking()); // recycled by offer()

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
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), 2);
             var b1G = new Guard.BatchGuard<TermBatch>(this);
             var b2G = new Guard.BatchGuard<TermBatch>(this);
             var b3G = new Guard.BatchGuard<TermBatch>(this);
             var b4G = new Guard.BatchGuard<TermBatch>(this);
             var b5G = new Guard.BatchGuard<TermBatch>(this)) {
            var b1 = b1G.set(intsBatch(1));
            var b2 = b2G.set(intsBatch(2));
            var b3 = b3G.set(intsBatch(3, 4));
            var b4 = b4G.set(intsBatch(5, 6));
            assertTimeout(Duration.ofMillis(5), () -> {
                it.offer(b1G.take());
                it.offer(b2G.take());
            }); // it: [1] [2]
            // start a offer(), which will block
            CompletableFuture<TermBatch> f3 = new CompletableFuture<>();
            Thread.startVirtualThread(() -> {
                assertDoesNotThrow(() -> it.offer(b3G.take()));
                f3.complete(null);
            });
            assertThrows(TimeoutException.class, () -> f3.get(50, MILLISECONDS));

            // consuming will unblock offer()
            assertSame(b1, b1G.set(it.nextBatch(null))); // it: [2] [3, 4]?
            assertSame(b2, b2G.set(it.nextBatch(null))); // it: [3, 4]?
            assertNull(f3.get()); // offer() was unblocked, it: [3, 4]

            // copy() must block() since b3 on filling has 2 items
            Thread t4 = Thread.startVirtualThread(
                    () -> assertDoesNotThrow(() -> it.copy(b4)));
            assertFalse(t4.join(ofMillis(50))); // it: [3, 4]

            // consuming will unblock() copy
            assertSame(b3, b3G.set(it.nextBatch(null))); // it: ø
            assertTrue(t4.join(ofMillis(50))); // it: [5, 6]

            // offer() and copy() are visible with order retained
            TermBatch last = b5G.set(it.nextBatch(null));
            assertEquals(b4, last);
            assertNotSame(b4, last);
        }
    }

    @Test void testMinWait() {
        int delay = 20;
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), maxItems);
             var b1G = new Guard.BatchGuard<>(intsBatch(1), this);
             var bGuard = new Guard.BatchGuard<TermBatch>(this)) {
            it.minBatch(2).minWait(delay, MILLISECONDS);
            long start = nanoTime();
            assertDoesNotThrow(() -> it.offer(b1G.take()));
            IntsBatch.offer(it, intsBatch(2));
            IntsBatch.offer(it, intsBatch(3));
            var b = bGuard.set(it.nextBatch(null));
            double ms = (nanoTime()-start)/1_000_000.0;
            assertTrue(Math.abs(ms-delay) < 10, "elapsed="+ms+" not in "+delay+"±10");
            assertEquals(intsBatch(1, 2, 3), b);

            // do not wait if batch is last
            start = nanoTime();
            IntsBatch.offer(it, intsBatch(4));
            it.complete(null);
            b = bGuard.set(it.nextBatch(null));
            ms = (nanoTime()-start)/1_000_000.0;
            assertEquals(intsBatch(4), b);
            assertTrue(ms < 10, "too slow, ms="+ms);

            // do not wait after end
            start = nanoTime();
            b = bGuard.set(it.nextBatch(null));
            ms = (nanoTime()-start)/1_000_000.0;
            assertNull(b);
            assertTrue(ms < 10, "too slow, ms="+ms);
        }
    }

    @Test void testMaxWait() throws Exception {
        int delay = 20, tolerance = 5;
        var ex1 = intsBatch(1).takeOwnership(this);
        var ex23 = intsBatch(2, 3).takeOwnership(this);
        List<TermBatch> batches = Collections.synchronizedList(new ArrayList<>());
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), maxItems)) {
            it.minBatch(2).maxWait(delay, MILLISECONDS);
            IntsBatch.offer(it, intsBatch(1));
            Thread thread  = Thread.ofVirtual().start(()
                    -> batches.add(Orphan.takeOwnership(it.nextBatch(null), this)));
            assertFalse(thread.join(ofMillis(tolerance)));
            busySleepMillis(delay);
            assertTrue(thread.join(ofMillis(tolerance)));

            IntsBatch.offer(it, intsBatch(2));
            IntsBatch.offer(it, intsBatch(3));
            assertTimeout(ofMillis(tolerance), ()
                    -> batches.add(Orphan.takeOwnership(it.nextBatch(null), this)));

            it.complete(null);
            assertTimeout(ofMillis(tolerance), ()
                    -> batches.add(Orphan.takeOwnership(it.nextBatch(null), this)));
            assertEquals(ex1, batches.get(0));
            assertEquals(ex23, batches.get(1));
            assertNull(batches.get(2));
            assertEquals(3, batches.size());
        } finally {
            ex1.recycle(this);
            ex23.recycle(this);
            for (TermBatch b : batches)
                Batch.safeRecycle(b, this);
        }
    }
}