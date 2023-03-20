package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.*;

class LevelPoolTest {
    private static class D {
        int thread, seq, capacity;
        boolean valid = true;

        public D(int thread, int capacity, int seq) {
            this.thread = thread;
            this.capacity = capacity;
            this.seq = seq;
        }

        @Override public String toString() {
            return (valid ? "(" : "INVALID(")+thread+", "+seq+")";
        }
    }

    @Test
    void testSingleThread() {
        int cap = 3, singletonCap = 23;
        var pool = new LevelPool<>(D.class, 3, 23);

        for (int capacity = 0x40000000; capacity > 0; capacity >>>= 1) {
            for (int i = 0, width = capacity == 1 ? singletonCap : cap; i < width; i++)
                assertNull(pool.offer(new D(0, capacity, i), capacity));
        }
        for (int capacity = 0x40000000; capacity > 0; capacity >>>= 1) {
            for (int i = 0, width = capacity == 1 ? singletonCap : cap; i < width; i++) {
                D d = new D(1, capacity, i);
                assertSame(d, pool.offer(d, capacity));
            }
        }
        for (int capacity = 0x40000000; capacity > 0; capacity >>>= 1) {
            for (int i = 0, width = capacity == 1 ? singletonCap : cap; i < width; i++) {
                String ctx = "capacity=0x"+Integer.toHexString(capacity)+", i="+i;
                D d = pool.get(capacity);
                assertNotNull(d, ctx);
                assertEquals(0, d.thread, "thread 1 offers shouldn't be visible "+ctx);
                assertEquals(capacity, d.capacity, ctx);
                assertEquals(width-1-i, d.seq, "expected LIFO policy "+ctx);
            }
        }
        for (int capacity = 0x40000000; capacity > 0; capacity >>>= 1) {
            assertNull(pool.get(capacity), "capacity="+capacity);
        }
    }


    @RepeatedTest(4)
    void testConcurrentWithLoss() throws Exception {
        int threadItems = 500_000;
        int capacity = 4;
        LevelPool<D> pool = new LevelPool<>(D.class, capacity, capacity);
        AtomicInteger accepted = new AtomicInteger();
        List<D> taken = Collections.synchronizedList(new ArrayList<>(threadItems));
        List<D> objects = new ArrayList<>(threadItems);
        for (int i = 0; i < threadItems; i++)
            objects.add(new D(0, (i&1) == 1 ? 10 : 1, i));

        try (var exec = newFixedThreadPool(2)) {
            List<Future<?>> tasks = new ArrayList<>();
            tasks.add(exec.submit(() -> {
                int threadAccepted = 0;
                for (int i = 0; i < threadItems; i++) {
                    D d = objects.get(i);
                    if (pool.offer(d, d.capacity) == null)
                        threadAccepted++;
                    else
                        d.valid = false; // fail if get() returns d
                }
                accepted.addAndGet(threadAccepted);
            }));
            tasks.add(exec.submit(() -> {
                List<D> threadTaken = new ArrayList<>(threadItems);
                for (int i = 0; i < threadItems; i++) {
                    D d = pool.get((i&1)==1 ? 10 : 1);
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
        var pool = new LevelPool<>(D.class, threads, threads);
        try (var exec = newFixedThreadPool(threads)) {
            List<Future<?>> tasks = new ArrayList<>();
            List<D> objects = new ArrayList<>();
            for (int thread = 0; thread < threads; thread++) {
                for (int i = 0; i < threadItems; i++)
                    objects.add(new D(thread, (i&1)==1 ? 2 : 1, i));
            }
            List<D> taken = Collections.synchronizedList(new ArrayList<>());
            for (int t = 0; t < threads; t++) {
                int tFinal = t;
                var threadTaken = new ArrayList<D>(threadItems);
                tasks.add(exec.submit(() -> {
                    for (int i = tFinal*threadItems, end = i+threadItems; i < end; i++) {
                        D offer = objects.get(i);
                        assertNull(pool.offer(offer, offer.capacity));
                        D d = pool.get(offer.capacity);
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