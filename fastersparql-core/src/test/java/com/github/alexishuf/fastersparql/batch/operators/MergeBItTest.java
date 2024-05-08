package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.*;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.util.IntList;
import com.github.alexishuf.fastersparql.util.concurrent.DebugJournal;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import com.github.alexishuf.fastersparql.util.concurrent.Watchdog;
import com.github.alexishuf.fastersparql.util.owned.Guard.BatchGuard;
import com.github.alexishuf.fastersparql.util.owned.Guard.ItGuard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.*;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static java.lang.System.nanoTime;
import static java.lang.Thread.ofVirtual;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;

class MergeBItTest extends AbstractMergeBItTest {
    private static final Logger log = LoggerFactory.getLogger(MergeBItTest.class);

    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new MergeBIt<>(s.operands(), TERM, X)) {
            s.drainer().drainUnordered(bit, s.expectedInts(), s.error());
        }
    }

    private int itQueueRows() {
        return FSProperties.itQueueRows(TERM, 1);
    }

    @ParameterizedTest @ValueSource(ints = {3, 3*2, 3*4, 3*8, 3*16, 3*100})
    void testCloseBlockedDrainers(int n) throws Exception {
        for (int repetition = 0; repetition < 16; repetition++) {
            var sources = range(0, n).mapToObj(_ -> new SPSCBIt<>(TERM, X, itQueueRows())).toList();
            var active = new AtomicBoolean(true);
            Thread feeder = null;
            try (var g = new ItGuard<>(this, new MergeBIt<>(sources, TERM, X))) {
                g.it.minBatch(2).minWait(10, MILLISECONDS);
                if (n <= 3) {
                    g.it.maxWait(20, MILLISECONDS);
                    offer(sources.getFirst(), 1);
                } else {
                    feeder = ofVirtual().name("feeder").start(() -> range(0, n - 3).forEach(i -> {
                        if (active.get())
                            offer(sources.get(i), 1);
                        if (active.get())
                            offer(sources.get(i + 1), 2, 3);
                        range(0, 3).forEach(x -> {
                            if (active.get())
                                offer(sources.get(i + 2), x);
                        });
                    }));
                }

                TermBatch batch = g.nextBatch();
                assertNotNull(batch);
                if (n <= 3)
                    assertEquals(1, batch.totalRows());
                else
                    assertTrue(batch.totalRows() >= 2);
            } // closes it
            active.set(false);
            if (feeder != null)
                feeder.join();
            assertTrue(sources.stream().allMatch(i -> i.state() == BIt.State.CANCELLED));
        }
    }

    @Test void testMinWait() {
        int wait = 50;
        try (var s1 = new SPSCBIt<>(TERM, X, itQueueRows());
             var s2 = new SPSCBIt<>(TERM, X, itQueueRows());
             var s3 = new SPSCBIt<>(TERM, X, itQueueRows());
             var ex = new BatchGuard<>(intsBatch(1, 2, 3), this);
             var g = new ItGuard<>(this, new MergeBIt<>(List.of(s1, s2, s3), TERM, X))) {
            g.it.minBatch(3).minWait(wait, MILLISECONDS);

            // single-thread, single-source wait
            long start = nanoTime();
            for (int i = 1; i <= 3; i++) offer(s1, i);
            TermBatch batch = g.nextBatch();
            double ms = (nanoTime() - start) / 1_000_000.0;
            assertEquals(ex.get(), batch);
            assertTrue(ms > wait-10, "elapsec="+ms+" not above minWait="+wait);
        }
    }

    @Test void testMinWaitMerging() {
        int wait = 50, tol = 20;
        try (var s1 = new SPSCBIt<>(TERM, X, itQueueRows());
             var s2 = new SPSCBIt<>(TERM, X, itQueueRows());
             var s3 = new SPSCBIt<>(TERM, X, itQueueRows());
             var it = new MergeBIt<>(List.of(s1, s2, s3), TERM, X);
             var exGuard = new BatchGuard<>(intsBatch(11, 12, 13, 14), this);
             var acGuard = new BatchGuard<TermBatch>(this)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // single-thread, two-sources wait
            offer(s2, 11, 12);
            offer(s3, 13);
            offer(s3, 14);
            long start = nanoTime();
            var ac = acGuard.set(it.nextBatch(null));
            double ms = (nanoTime() - start) / 1_000_000.0;
            assertNotNull(ac);
            assertEquals(new HashSet<>(exGuard.get().asList()),
                         new HashSet<>(ac.asList()));
            assertTrue(ms > wait-tol, "elapsed="+ms+" <= "+wait+"-"+tol);
        }
    }

    @Test void testMinWaitMergingConcurrent() throws Exception {
        int wait = 50, tol = wait/2;
        try (var s1 = new SPSCBIt<>(TERM, X, itQueueRows());
             var s2 = new SPSCBIt<>(TERM, X, itQueueRows());
             var s3 = new SPSCBIt<>(TERM, X, itQueueRows());
             var it = new MergeBIt<>(List.of(s1, s2, s3), TERM, X);
             var ex = new BatchGuard<>(intsBatch(21, 22, 23), this);
             var ac = new BatchGuard<TermBatch>(this)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // multi-thread, multi-source wait
            CompletableFuture<Orphan<TermBatch>> future = new CompletableFuture<>();
            long start = nanoTime();
            ofVirtual().start(() -> {
                try {
                    future.complete(it.nextBatch(null));
                } catch (Throwable t) { future.completeExceptionally(t); }
            });
            ofVirtual().start(() -> offer(s1, 21));
            ofVirtual().start(() -> offer(s2, 22));
            ofVirtual().start(() -> offer(s3, 23));
            ac.set(future.get());
            double ms = (nanoTime()-start)/1_000_000.0;
            assertTrue(Math.abs(ms-wait) < tol, "elapsed="+ms+" more than 50% off "+wait);
            assertEquals(3, ac.get().totalRows());
            assertEquals(new HashSet<>(ex.get().asList()),
                         new HashSet<>(ac.get().asList()));
        }
    }

    @Test void testMaxWait() {
        int min = 20, max = 100;
        try (var s1 = new SPSCBIt<>(TERM, X, itQueueRows());
             var s2 = new SPSCBIt<>(TERM, X, itQueueRows());
             var it = new MergeBIt<>(List.of(s1, s2), TERM, X);
             var ex = new BatchGuard<>(intsBatch(1), this);
             var ac = new BatchGuard<TermBatch>(this);
             var last = new BatchGuard<TermBatch>(this)) {
            it.minBatch(2).minWait(min, MILLISECONDS).maxWait(max, MILLISECONDS);

            offer(s1, 1);
            long start = nanoTime();
            ac.set(it.nextBatch(null));
            double elapsedMs = (nanoTime() - start) / 1_000_000.0;
            assertTrue(elapsedMs > max - 40 && elapsedMs < max + 40,
                       "elapsedMs="+elapsedMs+" not in (" + min + "," + max + ") range");
            assertEquals(ex.get(), ac.get());

            s1.complete(null);
            s2.complete(null);
            assertTimeout(ofMillis(5), () -> last.set(it.nextBatch(null)));
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
        var s0 = new SingletonBIt<>(intsBatch(1, 2), TERM, X);
        var s1 = new SPSCBIt<>(TERM, X, 2);
        try (var b0 = new BatchGuard<>(intsBatch(1, 2), this);
             var b1 = new BatchGuard<>(intsBatch(3), this);
             var ac = new BatchGuard<TermBatch>(this);
             var it = new MergeBIt<>(List.of(s0, s1), TERM, X)) {
            ac.set(it.nextBatch(null));
            assertEquals(b0.get(), ac.get());

            Thread.startVirtualThread(() ->  assertDoesNotThrow(() -> s1.offer(b1.take())));
            ac.set(it.nextBatch(null));
            assertEquals(b1.get(), ac.get());

            Thread.startVirtualThread(() -> s1.complete(null));
            assertNull(it.nextBatch(null));
        }
    }

    protected static Stream<Arguments> generatorData() {
        return BItGenerator.GENERATORS.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("generatorData")
    void testDoNotSplitBatches(BItGenerator gen) {
        var expected = List.of(0, 1, 10, 11);
        var expectedSet = new HashSet<>(expected);
        var actual = new ArrayList<Integer>(expected.size());
        var actualSet = new HashSet<>();
        for (int round = 0; round < 512; round++) {
            int delay = 10;
            var sources = List.of(
                    gen.asBIt(i -> i.minBatch(2).maxBatch(2), ints(0, 2)),
                    gen.asBIt(i -> i.minBatch(3).maxBatch(3), ints(5, 0)),
                    gen.asBIt(i -> i.minBatch(2).maxBatch(2), ints(10, 2)));
            List<TermBatch> batches = new ArrayList<>();
            try (var guard = new ItGuard<>(this, new MergeBIt<>(sources, TERM, X))) {
                guard.it.minBatch(2).minWait(delay, TimeUnit.MILLISECONDS);
                for (TermBatch b; (b = guard.nextBatch()) != null; )
                    batches.add(b);

                // check results are valid
                actual.clear();
                actualSet.clear();
                boolean split = false;
                for (TermBatch batch : batches) {
                    split |= batch.totalRows() < 2;
                    for (var node = batch; node != null; node = node.next) {
                        for (int r = 0; r < node.rows; r++)
                            actual.add(IntsBatch.parse(node.get(r, 0)));
                    }
                }
                actualSet.addAll(actual);
                assertEquals(expected.size(), actual.size());
                assertEquals(expectedSet, actualSet);

                // check for batch splitting
                if (split)
                    fail("Some batches were split. batches="+batches);
            } finally {
                for (TermBatch b : batches)
                    b.recycle(this);
            }
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
                            try {
                                cb.offer(batches[i].releaseOwnership(Feeder.this));
                            } catch (TerminatedException|CancelledException ignored) {}
                            batches[i] = null;
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
            Unparker.unpark(thread);
            for (TermBatch b : batches) {
                if (b != null) b.recycle(this);
            }
        }

        public void reset() {
            //if (journal.isClosed()) journal = DebugJournal.SHARED.role(name);
            //journal.write("begin reset()");
            assertFalse(isFeeding());
            for (int i = 0, val = begin; i < batches.length; i++, ++val) {
                TermBatch b = batches[i];
                if (b == null) {
                    //journal.write("pooled batch");
                    batches[i] = b = TERM.create(1).takeOwnership(Feeder.this);
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
            Unparker.unpark(thread);
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
        IntList consumed = new IntList(7);
        try (var s1Feeder = new Feeder("s1Feeder", 3, 6);
             var s2Feeder = new Feeder("s2Feeder", 6, 8);
             var watchdog = new Watchdog(() -> DebugJournal.SHARED.dump(50) )) {
            for (int round = 0, rounds = 100_000; round < rounds; round++) {
                if ((round & 1_023) == 0) log.info("round {}/{}", round, rounds);
                if ((round &    15) == 0) DebugJournal.SHARED.closeAll();
                watchdog.start(1_000_000_000L);
                var s0 = new IteratorBIt<>(s0Batches, TERM, X);
                var s1 = new SPSCBIt<>(TERM, X, 2);
                var s2 = new SPSCBIt<>(TERM, X, 1);
                sources.clear(); sources.add(s0); sources.add(s1); sources.add(s2);
                s1Feeder.reset();  s2Feeder.reset();
                for (TermBatch s1b : s1Feeder.batches) {
                    for (TermBatch s2b : s2Feeder.batches)
                        if (s1b != null && s2b != null) assertNotSame(s1b, s2b);
                }

                try (var g = new ItGuard<>(this, new MergeBIt<>(sources, TERM, X))) {
                    ((MergeBIt<TermBatch>)g.it).maxReadyItems(2);
                    s1Feeder.feed(s1);
                    s2Feeder.feed(s2);
                    switch (round & 31) {
                        case 4 -> Thread.yield();
                        case 8 -> LockSupport.parkNanos(100_000);
                        case 16 -> { try { Thread.sleep(1); } catch (Throwable ignored) { } }
                    }
                    // drain MergeBIt

                    for (TermBatch batch; (batch = g.nextBatch()) != null; ) {
                        for (var node = batch; node != null; node = node.next) {
                            for (int r = 0; r < node.rows; r++) {
                                var local = Objects.requireNonNull(node.get(r, 0)).local();
                                consumed.add(local.get(1) - '@');
                            }
                        }
                    }
                    boolean s1running = s1Feeder.await(10_000_000);
                    boolean s2running = s2Feeder.await(10_000_000);

                    // check consumed items
                    assertEqualsUnordered(expected, consumed,
                            false, false, false);
                    assertTrue(s1running, "s1Feeder feeding after consumer exhausted");
                    assertTrue(s2running, "s2Feeder feeding after consumer exhausted");

                    for (TermBatch s1b : s1Feeder.batches) {
                        for (TermBatch s2b : s2Feeder.batches)
                            if (s1b != null && s2b != null) assertNotSame(s1b, s2b);
                    }
                } catch (Throwable e) {
                    if (e instanceof ArrayIndexOutOfBoundsException)
                        System.err.println("consumed: "+consumed);
                    watchdog.stop();
                    DebugJournal.SHARED.dump(70);
                    throw e;
                } finally {
                    consumed.clear();
                }
            }
        }
    }
}