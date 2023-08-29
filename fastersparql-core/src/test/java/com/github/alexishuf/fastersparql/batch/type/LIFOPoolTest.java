package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.*;

class LIFOPoolTest {
    private static class D {
        final int thread, seq;
        boolean valid = true;

        public D(int thread, int seq) {
            this.thread = thread;
            this.seq = seq;
        }

        @Override public String toString() {
            return (valid ? "(" : "INVALID(")+thread+", "+seq+")";
        }
    }

    @Test
    void testSingleThread() {
        var pool = new LIFOPool<>(D.class, 23);
        for (int i = 0; i < 23; i++)
            assertNull(pool.offer(new D(0, i)), "i="+i);
        D last = new D(0, 23);
        assertSame(last, pool.offer(last));

        for (int i = 22; i >= 0; i--) {
            D d = pool.get();
            assertNotNull(d);
            assertEquals(i, d.seq);
        }
        assertNull(pool.get());
    }


    @RepeatedTest(4)
    void testConcurrentWithLoss() throws Exception {
        int threadItems = 500_000;
        int capacity = 8;
        LIFOPool<D> pool = new LIFOPool<>(D.class, capacity);
        AtomicInteger accepted = new AtomicInteger();
        List<D> taken = Collections.synchronizedList(new ArrayList<>(threadItems));
        List<D> objects = new ArrayList<>(threadItems);
        for (int i = 0; i < threadItems; i++)
            objects.add(new D(0, i));

        try (var exec = newFixedThreadPool(2)) {
            List<Future<?>> tasks = new ArrayList<>();
            tasks.add(exec.submit(() -> {
                int threadAccepted = 0;
                for (int i = 0; i < threadItems; i++) {
                    D d = objects.get(i);
                    if (pool.offer(d) == null)
                        threadAccepted++;
                    else
                        d.valid = false; // fail if get() returns d
                }
                accepted.addAndGet(threadAccepted);
            }));
            tasks.add(exec.submit(() -> {
                List<D> threadTaken = new ArrayList<>(threadItems);
                for (int i = 0; i < threadItems; i++) {
                    D d = pool.get();
                    if (d != null) threadTaken.add(d);
                }
                taken.addAll(threadTaken);
            }));
            for (Future<?> task : tasks) task.get();

            assertTrue(accepted.get() > capacity, "accepted="+accepted);
            assertTrue(taken.size() > capacity, "taken="+taken);
            assertTrue(taken.size() <= accepted.get(), "more non-null get()s than true offer()s");
            assertEquals(List.of(), taken.stream().filter(d -> !d.valid).toList(),
                         "invalidated objects returned by get()");
            Map<D, Integer> histogram = new HashMap<>();
            for (D d : taken)
                histogram.put(d, histogram.getOrDefault(d, 0)+1);
            assertEquals(List.of(),
                         histogram.keySet().stream().filter(k -> histogram.get(k) > 1).toList(),
                         "items get()ed more than once");
        }
    }


    @RepeatedTest(4) void testConcurrentNoLoss() throws Exception {
        int threads = getRuntime().availableProcessors();
        int threadItems = 500_000/threads;
        var pool = new LIFOPool<>(D.class, threads);
        try (var exec = newFixedThreadPool(threads)) {
            List<Future<?>> tasks = new ArrayList<>();
            List<D> objects = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                for (int i = 0; i < threadItems; i++)
                    objects.add(new D(thread, i));
            }
            List<D> taken = Collections.synchronizedList(new ArrayList<>());
            for (int t = 0; t < threads; t++) {
                int tFinal = t;
                var threadTaken = new ArrayList<D>(threadItems);
                tasks.add(exec.submit(() -> {
                    for (int i = tFinal*threadItems, end = i+threadItems; i < end; i++) {
                        assertNull(pool.offer(objects.get(i)));
                        D d = pool.get();
                        assertNotNull(d);
                        threadTaken.add(d);
                    }
                    taken.addAll(threadTaken);
                }));
            }
            for (Future<?> task : tasks) task.get();

            Map<D, Integer> histogram = new HashMap<>();
            for (D d : taken)
                histogram.put(d, histogram.getOrDefault(d, 0)+1);
            assertEquals(List.of(),
                         histogram.keySet().stream().filter(d -> histogram.get(d) > 1).toList(),
                         "objects get()ed by more than one thread");
        }
    }

}