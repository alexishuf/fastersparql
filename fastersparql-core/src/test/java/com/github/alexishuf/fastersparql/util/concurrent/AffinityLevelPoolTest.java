package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Runtime.getRuntime;
import static java.lang.Thread.onSpinWait;
import static org.junit.jupiter.api.Assertions.*;

class AffinityLevelPoolTest {
    private static final AtomicInteger nextId = new AtomicInteger(1);

    record C(int capacity, int id) {
        public C(int capacity) {
            this(capacity, nextId.getAndIncrement());
        }
    }

    @Test
    void testZero() {
        var a = new AffinityLevelPool<>(new LevelPool<>(C.class, 1, 1, 1, 1, 1));
        C zero = new C(0);
        assertNull(a.offer(zero, 0));
        assertSame(zero, a.getAtLeast(0));
        assertNull(a.getAtLeast(0));
    }

    @Test
    void testFloor() {
        var a = new AffinityLevelPool<>(new LevelPool<>(C.class, 1, 1, 1, 1, 1));

        C c = new C(7);
        assertNull(a.offer(c, 7));
        assertSame(c, a.getAtLeast(4));
        assertNull(a.getAtLeast(4));

        assertNull(a.offer(c, 7));
        assertSame(c, a.getAtLeast(3));
        assertNull(a.getAtLeast(3));
    }

    @ParameterizedTest @ValueSource(ints = {16, 1024, 8192})
    void testConcurrent(int capacity) throws ExecutionException, InterruptedException {
        LevelPool<C> levelPool = new LevelPool<>(C.class, 2, 4, 2, 1, 1);
        var lp = new AffinityLevelPool<>(levelPool);
        int threads = getRuntime().availableProcessors() * 2;
        int rounds = 100_000, objects = rounds * threads;
        List<C> offers = new ArrayList<>(objects), taken = new ArrayList<>(objects*2);
        for (int i = 0; i < objects;   i++) offers.add(new C(capacity));
        for (int i = 0; i < objects*2; i++) taken.add(null);
        try (var ex = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> tasks = new ArrayList<>(threads);
            for (int threadIdx = 0; threadIdx < threads; threadIdx++) {
                int thread = threadIdx;
                tasks.add(ex.submit(() -> {
                    int offerBase = thread*rounds;
                    int takenBase = thread*rounds*2;
                    for (int i = 0; i < rounds; i++) {
                        while (lp.offer(offers.get(offerBase + i), capacity) != null) onSpinWait();
                        taken.set(takenBase+i, lp.getAtLeast(capacity));
                    }
                    takenBase += rounds;
                    for (int i = 0; i < rounds; i++)
                        taken.set(takenBase+i, lp.getAtLeast(capacity));
                }));
            }
            for (Future<?> t : tasks)
                t.get();
        }

        List<C> nonNull = new ArrayList<>(objects);
        for (C c : taken) {
            if (c != null) nonNull.add(c);
        }
        HashSet<C> distinctNonNull = new HashSet<>(nonNull);

        assertEquals(nonNull.size(), distinctNonNull.size(),
                     "some items were taken more than once");
        List<C> missing = offers.stream().filter(o -> !distinctNonNull.contains(o)).toList();
        assertTrue(missing.isEmpty());
        if (nonNull.size() < offers.size())
            fail("Some items were offered but never taken");
        if (nonNull.size() > offers.size())
            fail("More items taken than offered");
    }

}