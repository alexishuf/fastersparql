package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.adapters.BatchGetter;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.System.nanoTime;
import static java.lang.Thread.ofVirtual;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;

class MergeBItTest extends AbstractMergeBItTest {
    @Override protected void run(Scenario scenario) {
        MergeScenario s = (MergeScenario) scenario;
        try (var bit = new MergeBIt<>(s.operands(), Integer.class, Vars.EMPTY)) {
            s.drainer().drainUnordered(bit, s.expected(), s.error());
        }
    }

    @ParameterizedTest @ValueSource(ints = {3, 3*2, 3*4, 3*8, 3*16, 3*100})
    void testCloseBlockedDrainers(int n) throws Exception {
        for (int repetition = 0; repetition < 16; repetition++) {
            var sources = range(0, n).mapToObj(i -> new CallbackBIt<>(Integer.class, Vars.EMPTY)).toList();
            var active = new AtomicBoolean(true);
            Thread feeder = null;
            try (var it = new MergeBIt<>(sources, Integer.class, Vars.EMPTY)) {
                it.minBatch(2).minWait(10, MILLISECONDS);
                if (n <= 3) {
                    it.maxWait(20, MILLISECONDS);
                    sources.get(0).feed(1);
                } else {
                    feeder = ofVirtual().name("feeder").start(() -> range(0, n - 3).forEach(i -> {
                        if (active.get())
                            sources.get(i).feed(1);
                        if (active.get())
                            sources.get(i + 1).feed(new Batch<>(new Integer[]{2, 3}, 2));
                        range(0, 3).forEach(x -> {
                            if (active.get())
                                sources.get(i + 2).feed(x);
                        });
                    }));
                }
                Batch<Integer> batch = it.nextBatch();
                if (n <= 3)
                    assertEquals(1, batch.size());
                else
                    assertTrue(batch.size() >= 2);
            } // closes it
            active.set(false);
            if (feeder != null)
                feeder.join();
            assertTrue(sources.stream().allMatch(AbstractBIt::isClosed));
        }
    }

    @ParameterizedTest @MethodSource("timingReliableBatchGetters")
    void testMinWait(BatchGetter getter) {
        int wait = 50, tol = wait/2;
        try (var s1 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var s2 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var s3 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var it = new MergeBIt<>(List.of(s1, s2, s3), Integer.class, Vars.EMPTY)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // single-thread, single-source wait
            long start = nanoTime();
            for (int i = 1; i <= 3; i++) s1.feed(i);
            assertEquals(List.of(1, 2, 3), getter.getList(it));
            double ms = (nanoTime() - start) / 1_000_000.0;
            assertTrue(Math.abs(ms-wait) < tol, "elapsed="+ms+" not in "+wait+"±10");
        }
    }

    @ParameterizedTest @MethodSource("timingReliableBatchGetters")
    void testMinWaitMerging(BatchGetter getter) {
        int wait = 50, tol = wait/2;
        try (var s1 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var s2 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var s3 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var it = new MergeBIt<>(List.of(s1, s2, s3), Integer.class, Vars.EMPTY)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // single-thread, two-sources wait
            s2.feed(new Batch<>(new Integer[]{11, 12}, 2));
            s3.feed(13);
            s3.feed(14);
            long start = nanoTime();
            List<Integer> batch = getter.getList(it);
            double ms = (nanoTime() - start) / 1_000_000.0;
            assertEquals(Set.of(11, 12, 13, 14), new HashSet<>(batch));
            assertTrue(Math.abs(ms-wait) < tol, "elapsed="+ms+" not in "+wait+"±10");
        }
    }

    @ParameterizedTest @MethodSource("timingReliableBatchGetters")
    void testMinWaitMergingConcurrent(BatchGetter getter) throws Exception {
        int wait = 50, tol = wait/2;
        try (var s1 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var s2 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var s3 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var it = new MergeBIt<>(List.of(s1, s2, s3), Integer.class, Vars.EMPTY)) {
            it.minBatch(3).minWait(wait, MILLISECONDS);

            // multi-thread, multi-source wait
            CompletableFuture<List<Integer>> future = new CompletableFuture<>();
            long start = nanoTime();
            ofVirtual().start(() -> {
                try {
                    future.complete(getter.getList(it));
                } catch (Throwable t) { future.completeExceptionally(t); }
            });
            ofVirtual().start(() -> s1.feed(21));
            ofVirtual().start(() -> s2.feed(22));
            ofVirtual().start(() -> s3.feed(new Batch<>(new Integer[]{23}, 1)));
            List<Integer> batch = future.get();
            double ms = (nanoTime()-start)/1_000_000.0;
            assertTrue(Math.abs(ms-wait) < tol, "elapsed="+ms+" more than 50% off "+wait);
            assertEquals(3, batch.size());
            assertEquals(Set.of(21, 22, 23), new HashSet<>(batch));
        }
    }

    @ParameterizedTest @MethodSource("timingReliableBatchGetters")
    void testMaxWait(BatchGetter getter) {
        int min = 20, max = 30;
        try (var s1 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var s2 = new CallbackBIt<>(Integer.class, Vars.EMPTY);
             var it = new MergeBIt<>(List.of(s1, s2), Integer.class, Vars.EMPTY)) {
            it.minBatch(2).minWait(min, MILLISECONDS).maxWait(max, MILLISECONDS);

            s1.feed(1);
            long start = nanoTime();
            List<Integer> batch = getter.getList(it);
            double elapsedMs = (nanoTime() - start) / 1_000_000.0;
            assertTrue(elapsedMs > min - 5 && elapsedMs < max + 5,
                       "elapsedMs not in (" + min + "," + max + ") range");
            assertEquals(List.of(1), batch);


            CompletableFuture<List<Integer>> empty = new CompletableFuture<>();
            s1.complete(null);
            s2.complete(null);
            assertTimeout(ofMillis(5), () -> empty.complete(getter.getList(it)));
        }

    }
}