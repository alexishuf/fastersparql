package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.SingletonBIt;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.DebugJournal;
import com.github.alexishuf.fastersparql.util.concurrent.Watchdog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxBatches;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.*;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static java.lang.System.nanoTime;
import static java.lang.Thread.ofVirtual;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;

class MergeBItTest extends AbstractMergeBItTest {
    private static final Logger log = LoggerFactory.getLogger(MergeBItTest.class);
    private static final Vars X = Vars.of("x");

    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new MergeBIt<>(s.operands(), TERM, X)) {
            s.drainer().drainUnordered(bit, s.expectedInts(), s.error());
        }
    }

    @ParameterizedTest @ValueSource(ints = {3, 3*2, 3*4, 3*8, 3*16, 3*100})
    void testCloseBlockedDrainers(int n) throws Exception {
        for (int repetition = 0; repetition < 16; repetition++) {
            var sources = range(0, n).mapToObj(i -> new SPSCBIt<>(TERM, X, queueMaxBatches())).toList();
            var active = new AtomicBoolean(true);
            Thread feeder = null;
            try (var it = new MergeBIt<>(sources, TERM, X)) {
                it.minBatch(2).minWait(10, MILLISECONDS);
                if (n <= 3) {
                    it.maxWait(20, MILLISECONDS);
                    offerAndInvalidate(sources.get(0), 1);
                } else {
                    feeder = ofVirtual().name("feeder").start(() -> range(0, n - 3).forEach(i -> {
                        if (active.get())
                            offerAndInvalidate(sources.get(i), 1);
                        if (active.get())
                            offerAndInvalidate(sources.get(i + 1), 2, 3);
                        range(0, 3).forEach(x -> {
                            if (active.get())
                                offerAndInvalidate(sources.get(i + 2), x);
                        });
                    }));
                }

                TermBatch batch = it.nextBatch(null);
                assertNotNull(batch);
                if (n <= 3)
                    assertEquals(1, batch.rows);
                else
                    assertTrue(batch.rows >= 2);
            } // closes it
            active.set(false);
            if (feeder != null)
                feeder.join();
            assertTrue(sources.stream().allMatch(AbstractBIt::isClosed));
        }
    }

    @Test void testMinWait() {
        int wait = 50;
        try (var s1 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var s2 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var s3 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var it = new MergeBIt<>(List.of(s1, s2, s3), Batch.TERM, X)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // single-thread, single-source wait
            long start = nanoTime();
            for (int i = 1; i <= 3; i++) offerAndInvalidate(s1, i);
            TermBatch batch = it.nextBatch(null);
            double ms = (nanoTime() - start) / 1_000_000.0;
            assertEquals(intsBatch(1, 2, 3), batch);
            assertTrue(ms > wait-10, "elapsec="+ms+" not above minWait="+wait);
        }
    }

    @Test void testMinWaitMerging() {
        int wait = 50, tol = 20;
        try (var s1 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var s2 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var s3 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var it = new MergeBIt<>(List.of(s1, s2, s3), Batch.TERM, X)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // single-thread, two-sources wait
            offerAndInvalidate(s2, 11, 12);
            offerAndInvalidate(s3, 13);
            offerAndInvalidate(s3, 14);
            long start = nanoTime();
            TermBatch batch = it.nextBatch(null);
            double ms = (nanoTime() - start) / 1_000_000.0;
            assertNotNull(batch);
            assertEquals(new HashSet<>(intsBatch(11, 12, 13, 14).asList()),
                         new HashSet<>(batch.asList()));
            assertTrue(ms > wait-tol, "elapsed="+ms+" <= "+wait+"-"+tol);
        }
    }

    @Test void testMinWaitMergingConcurrent() throws Exception {
        int wait = 50, tol = wait/2;
        try (var s1 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var s2 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var s3 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var it = new MergeBIt<>(List.of(s1, s2, s3), Batch.TERM, X)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // multi-thread, multi-source wait
            CompletableFuture<TermBatch> future = new CompletableFuture<>();
            long start = nanoTime();
            ofVirtual().start(() -> {
                try {
                    future.complete(it.nextBatch(null));
                } catch (Throwable t) { future.completeExceptionally(t); }
            });
            ofVirtual().start(() -> offerAndInvalidate(s1, 21));
            ofVirtual().start(() -> offerAndInvalidate(s2, 22));
            ofVirtual().start(() -> offerAndInvalidate(s3, 23));
            TermBatch batch = future.get();
            double ms = (nanoTime()-start)/1_000_000.0;
            assertTrue(Math.abs(ms-wait) < tol, "elapsed="+ms+" more than 50% off "+wait);
            assertEquals(3, batch.rows);
            assertEquals(new HashSet<>(intsBatch(21, 22, 23).asList()),
                         new HashSet<>(batch.asList()));
        }
    }

    @Test void testMaxWait() {
        int min = 20, max = 100;
        try (var s1 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var s2 = new SPSCBIt<>(Batch.TERM, X, queueMaxBatches());
             var it = new MergeBIt<>(List.of(s1, s2), Batch.TERM, X)) {
            it.minBatch(2).minWait(min, MILLISECONDS).maxWait(max, MILLISECONDS);

            offerAndInvalidate(s1, 1);
            long start = nanoTime();
            var batch = it.nextBatch(null);
            double elapsedMs = (nanoTime() - start) / 1_000_000.0;
            assertTrue(elapsedMs > max - 40 && elapsedMs < max + 40,
                       "elapsedMs="+elapsedMs+" not in (" + min + "," + max + ") range");
            assertEquals(intsBatch(1), batch);

            CompletableFuture<TermBatch> empty = new CompletableFuture<>();
            s1.complete(null);
            s2.complete(null);
            assertTimeout(ofMillis(5), () -> empty.complete(it.nextBatch(null)));
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 4, 16, 256}) void testSingletons(int n) {
        var sources = range(0, n).mapToObj(i -> new SingletonBIt<>(intsBatch(i), TERM, X)).toList();
        var expected = range(0, n).mapToObj(i -> List.of(term(i))).toList();
        try (var it = new MergeBIt<>(sources, TERM, X)) {
            BItDrainer.RECYCLING.drainUnordered(it, expected, null);
        }
    }

    @Test void testOneSingletonOneCallback() {
        TermBatch b0 = intsBatch(1, 2);
        TermBatch b1 = intsBatch(3);
        var s0 = new SingletonBIt<>(b0, TERM, X);
        var s1 = new SPSCUnitBIt<>(TERM, X);
        try (var it = new MergeBIt<>(List.of(s0, s1), TERM, X)) {
            TermBatch b = it.nextBatch(null);
            assertEquals(intsBatch(1, 2), b);
            assertSame(b0, b);

            Thread.startVirtualThread(() -> s1.offer(b1));
            b = it.nextBatch(null);
            assertEquals(intsBatch(3), b);
            assertSame(b1, b);

            Thread.startVirtualThread(() -> s1.complete(null));
            assertNull(it.nextBatch(null));
        }
    }

    private static final class Feeder implements AutoCloseable {
        final String name;
        final int begin;
        final TermBatch[] batches;
        final Thread thread;
        volatile CallbackBIt<TermBatch> cb;
        volatile boolean stop;
//        private DebugJournal.RoleJournal journal;

        public Feeder(String name, int begin, int end) {
            this.name = name;
            //this.journal = DebugJournal.SHARED.role(name);
            this.begin = begin;
            this.batches = new TermBatch[end-begin];
            (this.thread = new Thread(() -> {
                LockSupport.park(this);
                while (!stop) {
                    if (cb != null) {
                        for (int i = 0; i < batches.length; i++) {
                            //journal.write("batch i=", i, "rows=", batches[i].rows, "[0][0]=", batches[i].get(0, 0));
                            batches[i] = cb.offer(batches[i]);
                            //journal.write("old &batch[i]=", old, " curr &batch[i]=", identityHashCode(batches[i]));
                        }
                        cb.complete(null);
                        cb = null;
                    }
                    LockSupport.park(this);
                }
            }, name)).start();
        }

        @Override public void close() {
            stop = true;
            LockSupport.unpark(thread);
        }

        public void reset() {
            //if (journal.isClosed()) journal = DebugJournal.SHARED.role(name);
            //journal.write("begin reset()");
            assertFalse(isFeeding());
            for (int i = 0, val = begin; i < batches.length; i++, ++val) {
                TermBatch b = batches[i];
                if (b == null) {
                    //journal.write("pooled batch");
                    batches[i] = b = TERM.createSingleton(1);
                }
                b.clear();
                b.beginPut();
                b.putTerm(0, IntsBatch.term(val));
                b.commitPut();
                //journal.write("&batches[i]=", System.identityHashCode(batches[i]), "val=", val, "[0][0]=", batches[i].get(0, 0));
            }
            //journal.write("end reset()");
        }

        public void feed(CallbackBIt<TermBatch> cb) {
            //journal.write("feed", cb, ": begin");
            assertNull(this.cb, "still feeding");
            this.cb = cb;
            //journal.write("feed", cb, ": unpark()ing");
            LockSupport.unpark(thread);
        }

        public boolean await(long nanos) {
            long start = nanoTime();
            while (nanoTime()-start < nanos && cb != null) Thread.yield();
            return cb == null;
        }

        public boolean isFeeding() { return cb != null; }

        @Override public String toString() { return name; }
    }

    @Test
    void testStarvation() {
        List<BIt<TermBatch>> sources = new ArrayList<>(3);
        var s0Batches = List.of(intsBatch(1), intsBatch(2));
        int[] expected = IntsBatch.ints(1, 7);
        int[] consumed = new int[7];
        try (var s1Feeder = new Feeder("s1Feeder", 3, 6);
             var s2Feeder = new Feeder("s2Feeder", 6, 8);
             var watchdog = new Watchdog(() -> DebugJournal.SHARED.dump(50) )) {
            for (int round = 0, rounds = 100_000; round < rounds; round++) {
                if ((round & 1_023) == 0) log.info("round {}/{}", round, rounds);
                if ((round &    15) == 0) DebugJournal.SHARED.reset();
                watchdog.start(1_000_000_000L);
                var s0 = new IteratorBIt<>(s0Batches, TERM, X);
                var s1 = new SPSCBIt<>(TERM, X, 2);
                var s2 = new SPSCUnitBIt<>(TERM, X);
                sources.clear(); sources.add(s0); sources.add(s1); sources.add(s2);
                s1Feeder.reset();  s2Feeder.reset();
                Arrays.fill(consumed, 0);
                for (TermBatch s1b : s1Feeder.batches) {
                    for (TermBatch s2b : s2Feeder.batches)
                        if (s1b != null && s2b != null) assertNotSame(s1b, s2b);
                }

                try (var it = new MergeBIt<>(sources, TERM, X, 2)) {
                    s1Feeder.feed(s1);
                    s2Feeder.feed(s2);
                    switch (round & 31) {
                        case 4 -> Thread.yield();
                        case 8 -> LockSupport.parkNanos(100_000);
                        case 16 -> { try { Thread.sleep(1); } catch (Throwable ignored) { } }
                    }
                    // drain MergeBIt
                    int nConsumed = 0;

                    for (TermBatch b = null; (b = it.nextBatch(b)) != null; ) {
                        for (int r = 0; r < b.rows; r++) {
                            byte[] local = Objects.requireNonNull(b.get(r, 0)).local;
                            consumed[nConsumed++] = local[1] - '0';
                        }
                    }
                    boolean s1running = s1Feeder.await(10_000_000);
                    boolean s2running = s2Feeder.await(10_000_000);

                    // check consumed items
                    assertEqualsUnordered(expected, consumed, nConsumed,
                            false, false, false);
                    assertTrue(s1running, "s1Feeder feeding after consumer exhausted");
                    assertTrue(s2running, "s2Feeder feeding after consumer exhausted");

                    for (TermBatch s1b : s1Feeder.batches) {
                        for (TermBatch s2b : s2Feeder.batches)
                            if (s1b != null && s2b != null) assertNotSame(s1b, s2b);
                    }
                } catch (Throwable e) {
                    if (e instanceof ArrayIndexOutOfBoundsException)
                        System.err.println("consumed: "+Arrays.toString(consumed));
                    watchdog.stop();
                    DebugJournal.SHARED.dump(70);
                    throw e;
                }
            }
        }
    }
}