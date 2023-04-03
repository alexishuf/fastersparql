package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.*;
import com.github.alexishuf.fastersparql.batch.adapters.AbstractBItTest;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.DebugJournal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.FSProperties.queueMaxBatches;
import static com.github.alexishuf.fastersparql.batch.BItGenerator.GENERATORS;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.*;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.client.util.TestTaskSet.platformRepeatAndWait;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class CallbackBItTest extends AbstractBItTest {

    protected abstract CallbackBIt<TermBatch> create(int capacity);

    @Test void testSimple() {
        try (var it = create(1)) {
            TermBatch b = intsBatch(1);
            assertNull(it.offer(b));
            assertSame(b, it.nextBatch(null));
            it.complete(null);
            assertNull(it.nextBatch(b));
        }
    }

    @Test void testCompleteBeforeExhausted() {
        try (var it = create(1)) {
            TermBatch b = intsBatch(1);
            assertNull(it.offer(b));
            it.complete(null);
            assertSame(b, it.nextBatch(null));
            assertNull(it.nextBatch(b));
        }
    }

    @Test void testFailBeforeExhausted() {
        try (var it = create(1)) {
            TermBatch b = intsBatch(1);
            assertNull(it.offer(b));
            var ex = new RuntimeException();
            it.complete(ex);
            assertSame(b, it.nextBatch(null));
            try {
                it.nextBatch(null);
                fail("Expected BItReadFailedException");
            } catch (BItReadFailedException e) {
                assertSame(ex, e.rootCause());
                assertSame(ex, e.getCause());
            }
        }
    }

    private void testRaceFeedNextAndClose(int round, int minBatch, int waitBatches) {
        CompletableFuture<?> feed = new CompletableFuture<>(), drain = new CompletableFuture<>();
//        var yieldAfter = 2* getRuntime().availableProcessors()*minBatch*waitBatches;
        var stop = new AtomicBoolean();
        var prematureExhaust = new AtomicBoolean(false);
        var suffix = format("{round=%d, min=%d, wait=%d}", round, minBatch, waitBatches);
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), queueMaxBatches())) {
            it.maxReadyItems(Math.max(65_536, 2*minBatch)).minBatch(minBatch);
            var batchDrained = new Semaphore(0);
            Thread.ofVirtual().name("Feeder"+suffix).start(() -> {
                try {
                    for (int i = 0; !stop.get(); i++) {
                        IntsBatch.offerAndInvalidate(it, intsBatch(i));
//                        if (i >= yieldAfter)
//                            Thread.yield();
                    }
                    feed.complete(null);
                } catch (BItCompletedException ignored) {
                    feed.complete(null);
                } catch (Throwable t) {
                    feed.completeExceptionally(t);
                }
            });
            Thread.ofVirtual().name("Drainer"+suffix).start(() -> {
                try {
                    for (TermBatch b = null; (b = it.nextBatch(b)) != null; )
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
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertFalse(prematureExhaust.get());
    }

    static Stream<Arguments> testRaceFeedNextAndClose() {
        List<Arguments> list = new ArrayList<>();
        for (int minBatch : List.of(1, 2, 3, 4, 16)) {
            for (int wait : List.of(0, 1, 2, 4))  list.add(arguments(minBatch, wait));
        }
        for (int minBatch : List.of(128, 1024)) {
            for (int wait : List.of(0, 1))  list.add(arguments(minBatch, wait));
        }
        return list.stream();
    }

    /** Concurrent calls to feed(), nextBatch() and close() */
    @ParameterizedTest @MethodSource
    void testRaceFeedNextAndClose(int minBatch, int waitBatches) throws Exception {
        int threads = 2*getRuntime().availableProcessors();
        String name = ".testRaceFeedNextAndClose("+minBatch+", "+waitBatches+")";
        try {
            testRaceFeedNextAndClose(-1, minBatch, waitBatches);
            platformRepeatAndWait(name, threads, (Consumer<Integer>) thread
                    -> testRaceFeedNextAndClose(thread, minBatch, waitBatches));
        } finally {
            System.gc();
        }
    }

    @ParameterizedTest @ValueSource(ints = {2, 4, 8})
    void testWrapCapacity(int capacity) throws Exception {
        int iteratorCount = 128;
        int[] ints = IntsBatch.ints(8*capacity);
        List<CallbackBIt<TermBatch>> iterators = new ArrayList<>(iteratorCount);

        for (int i = 0; i < iteratorCount; i++)
            iterators.add(new SPSCBIt<>(TERM, Vars.of("x"), capacity));
        Thread producer = Thread.ofPlatform().name("producer").unstarted(() -> {
            for (CallbackBIt<TermBatch> it : iterators) {
                offerAndInvalidate(it, ints);
                it.complete(null);
            }
        });
        CompletableFuture<List<int[]>> consumed = new CompletableFuture<>();
        Thread consumer = Thread.ofPlatform().name("consumer").unstarted(() -> {
            try {
                ArrayList<int[]> intsList = new ArrayList<>(iteratorCount);
                for (CallbackBIt<TermBatch> it : iterators)
                    intsList.add(BItDrainer.RECYCLING.drainToInts(it, ints.length));
                consumed.complete(intsList);
            } catch (Throwable t) { consumed.completeExceptionally(t); }
        });
        producer.start();
        consumer.start();
        producer.join();
        for (int[] actual : consumed.get())
            IntsBatch.assertEqualsOrdered(ints, actual, actual.length);
    }


    static Stream<Arguments> testLostItem() {
        List<Arguments> list = new ArrayList<>();
        for (BItDrainer drainer : BItDrainer.ALL) {
            for (int multiplier : List.of(1, 2, 3, 4))
                list.add(arguments(multiplier, drainer));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testLostItem(int multiplier, BItDrainer drainer) throws Exception {
        var lock = new ReentrantLock();
        int nSources = multiplier * GENERATORS.size();
        try (var exec = newFixedThreadPool(nSources);
             var tasks = new TestTaskSet("testLostItem("+multiplier+")", exec)) {
            for (int round = 0; round < 100; round++) {
                // create a SPSCBIt and feed from multiple threads, as done in MergeBIt
                // check no items are lost
                try (var cb = create(queueMaxBatches())) {
                    cb.minBatch(1).maxBatch(3);
                    var barrier1 = new AtomicInteger(nSources);
                    var barrier2 = new AtomicInteger(nSources);
                    var latch = new AtomicInteger(nSources);
                    for (int i = 0, start = 0; i < multiplier; i++) {
                        for (BItGenerator gen : GENERATORS) {
                            final int currentStart = start;
                            start += 2;
                            tasks.add(() -> {
                                Thread.currentThread().setName("feeder{start="+currentStart+"}");
//                                var journal = DebugJournal.SHARED.role("feeder{start=" + currentStart + "}");
                                // increase chance of collisions
                                try { Thread.sleep(1); } catch (Throwable ignored) {}
                                barrier1.getAndDecrement();
                                while (barrier1.get() > 0) Thread.onSpinWait();
                                try (var it = gen.asBIt(ints(currentStart, 2))) {
                                    barrier2.getAndDecrement();
                                    while (barrier2.get() > 0) Thread.onSpinWait();
                                    // drain it into cb
                                    for (TermBatch b = null; (b = it.nextBatch(b)) != null; ) {
                                        lock.lock(); // offer(
                                        try {
                                            //journal.write("offer b[0][0]=", b.get(0, 0).local[1]-'0', "size=", b.rows);
                                            b = cb.offer(b);
                                        } finally { lock.unlock(); }
                                    }
                                    //journal.write("exhausted");
                                    if (latch.decrementAndGet() == 0) {
                                        //journal.write("complete(null)");
                                        cb.complete(null);
                                    }
                                }
                            });
                        }
                    }
                    drainer.drainUnordered(cb, ints(nSources*2), null);
                    tasks.await();
                } catch (Throwable t) {
                    DebugJournal.SHARED.dump(40);
                    throw t;
                }
            }
        }
    }

    @Test void testTightWaitProgress() {
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), queueMaxBatches())) {
            it.minBatch(3).minWait(50, MICROSECONDS).maxWait(50, MICROSECONDS);
            IntsBatch.offerAndInvalidate(it, 1, 2);
            assertEquals(intsBatch(1, 2), it.nextBatch(null));
        }
    }

    @Test void testTightWait() {
        int wait = 50;
        try (var it = new SPSCBIt<>(TERM, Vars.of("x"), queueMaxBatches())) {
            it.minBatch(3).minWait(50, MILLISECONDS).maxWait(50, MILLISECONDS);
            IntsBatch.offerAndInvalidate(it, 1, 2);
            long start = nanoTime();
            var b = it.nextBatch(null);
            double ms = (nanoTime() - start) / 1_000_000.0;
            assertEquals(intsBatch(1, 2), b);
            assertTrue(ms < wait+10, "elapsed="+ms+"ms above "+wait+"+10 ms");
            assertTrue(ms > wait-10, "elapsed="+ms+"ms below "+wait+"-10 ms");
        }
    }
}
