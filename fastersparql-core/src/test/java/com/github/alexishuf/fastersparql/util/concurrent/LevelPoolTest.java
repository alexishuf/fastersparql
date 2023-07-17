package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.concurrent.LevelPool.HUGE_MAX_CAPACITY;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class LevelPoolTest {
    private static final AtomicInteger nextId = new AtomicInteger();
    record C(int id, int capacity) {
        public C(int capacity) {this(nextId.getAndIncrement(), capacity);}
    }

    private static final int[] CAPACITIES;
    private static final int[] INTERESTING_CAPACITIES;

    static {
        int[] capacities = new int[numberOfTrailingZeros(HUGE_MAX_CAPACITY)+1];
        for (int i = 1; i <= HUGE_MAX_CAPACITY; i<<=1)
            capacities[numberOfTrailingZeros(i)] = i;
        int[] interesting = new int[2+(capacities.length-2)*3+2];
        interesting[0] = capacities[0];
        interesting[1] = capacities[1];
        int o = 2;
        for (int i = 1; i < capacities.length-1; i++) {
            int c = capacities[i];
            interesting[o++] = c-1;
            interesting[o++] = c;
            interesting[o++] = c+1;
        }
        interesting[o++] = capacities[capacities.length-1]-1;
        interesting[o  ] = capacities[capacities.length-1];
        CAPACITIES = capacities;
        INTERESTING_CAPACITIES = interesting;
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3, 4, 257})
    void testSingleThread(int n) {
        LevelPool<C> pool = new LevelPool<>(C.class, n, n, n, n, n);
        List<C> objects = new ArrayList<>(n);
        Set<C> taken = new HashSet<>(n);

        C zero = new C(0);
        assertNull(pool.offer(zero, 0));
        assertSame(zero, pool.getAtLeast(0));
        assertNull(pool.getAtLeast(0));
        assertNull(pool.offerToNearest(zero, 0));
        assertSame(zero, pool.getAtLeast(0));

        for (int capacity : CAPACITIES) {
            objects.clear();
            for (int i = 0; i < n; i++) objects.add(new C(capacity));
            for (C o : objects)
                assertNull(pool.offer(o, o.capacity), "capacity="+capacity);
            taken.clear();
            for (int i = 0; i < n; i++) taken.add(pool.getAtLeast(capacity));
            assertNull(pool.getAtLeast(capacity));
            if (!taken.containsAll(objects))
                fail("Missing objects.\nExpected: "+objects+"\n Actual  : "+taken);
            if (taken.size() != objects.size())
                fail("Extraneous objects.\nExpected: "+objects+"\n Actual  : "+taken);
        }
    }

    @Test
    void testGetAtLeast() {
        LevelPool<C> pool = new LevelPool<>(C.class, 3, 3, 3, 3, 3);
        for (int c : CAPACITIES) {
            assertNull(pool.offer(new C(c), c));
            assertNull(pool.offer(new C(c), c));
            assertNull(pool.offer(new C(c), c));
        }
        HashSet<C> seen = new HashSet<>();
        for (int c : INTERESTING_CAPACITIES) {
            C o = pool.getAtLeast(c);
            assertNotNull(o);
            assertTrue(o.capacity >= c);
            if (!seen.add(o))
                fail("o="+o+" got duplicated");
        }
    }

    static Stream<Arguments> testConcurrent() {
        return Stream.of(
                arguments(0x0020, 1), // small,  narrow
                arguments(0x0400, 1), // medium, narrow
                arguments(0x2000, 1), // large,  narrow

                arguments(0x0020, 32), // small,  wide
                arguments(0x0400, 32), // medium, wide
                arguments(0x2000, 32)  // large,  wide
        );
    }

    @ParameterizedTest @MethodSource
    void testConcurrent(int capacity, int levelCapacity) throws Exception {
        int mediumCap = Math.max(1, levelCapacity>>1);
        int largeCap = Math.max(1, levelCapacity>>2);
        int hugeCap = Math.max(1, levelCapacity>>3);

        int threads = getRuntime().availableProcessors() * 2;
        int rounds = 4_000;
        int threadObjects = (levelCapacity+1)*rounds, totalObjects = threads*threadObjects;

        LevelPool<C> pool = new LevelPool<>(C.class, levelCapacity, levelCapacity, mediumCap, largeCap, hugeCap);
        List<C> objects = new ArrayList<>(totalObjects);
        for (int i = 0; i < totalObjects; i++)
            objects.add(new C(capacity));
        List<C> taken = new ArrayList<>(totalObjects+threadObjects);
        for (int i = 0; i < totalObjects; i++)
            taken.add(null);
        AtomicInteger acceptedOffers = new AtomicInteger();

        try (var exec = newFixedThreadPool(threads)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int threadIdx = 0; threadIdx < threads; threadIdx++) {
                int thread = threadIdx;
                tasks.add(exec.submit(() -> {
                    int accepted = 0;
                    for (int i = thread*threadObjects, e = i+threadObjects; i < e; i++) {
                        if (pool.offer(objects.get(i), capacity) == null)
                            accepted++;
                    }
                    acceptedOffers.addAndGet(accepted);
                }));
                tasks.add(exec.submit(() -> {
                    for (int i = thread*threadObjects, e = i+threadObjects; i < e; i++)
                        taken.set(i, pool.getAtLeast(capacity));
                }));
            }
            for (Future<?> t : tasks)
                t.get();
            tasks.clear();
            for (int threadIdx = 0; threadIdx < threads; threadIdx++)
                tasks.add(exec.submit(() -> pool.getAtLeast(capacity)));
            for (Future<?> t : tasks) {
                C o = (C)t.get();
                if (o != null) taken.add(o);
            }
        }
        // drain objects still in pool. consumers might end before producers, leaving these back
        for (C c; (c = pool.getAtLeast(capacity)) != null; )
            taken.add(c);
        assertNull(pool.getAtLeast(capacity), "non-null get() after get() == null without offers");

        List<C> nonNullTaken = new ArrayList<>(totalObjects);
        for (C c : taken) {
            if (c != null) nonNullTaken.add(c);
        }
        assertTrue(acceptedOffers.get() >= levelCapacity,
                   "acceptedOffers too low: "+acceptedOffers.get());
        assertEquals(new HashSet<>(nonNullTaken).size(), nonNullTaken.size(),
                     "some items were taken more than once");
        assertTrue(acceptedOffers.get() >= nonNullTaken.size(),
                   "more items were taken than offered");
        // we cannot asser that acceptedOffers == nonNullTaken because an offer might be stored
        // in a locals slot and no subsequent taking thread collides with the offering thread to
        // take that item. In some trial runs, usually this happens to only one item.
    }

}