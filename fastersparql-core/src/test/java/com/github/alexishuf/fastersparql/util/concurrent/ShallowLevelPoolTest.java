package com.github.alexishuf.fastersparql.util.concurrent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Runtime.getRuntime;
import static org.junit.jupiter.api.Assertions.*;

class ShallowLevelPoolTest {

    private static final AtomicInteger nextId = new AtomicInteger(1);
    private static final int STEAL_ATTEMPTS = ShallowLevelPool.STEAL_ATTEMPTS;

    static class C {
        final int id, capacity;
        public C(int capacity) {
            this.id = nextId.getAndIncrement();
            this.capacity = capacity;
        }
        @Override public String toString() { return String.format("C@%d[%d]", id, capacity); }
        @Override public int hashCode() {return id;}
        @Override public boolean equals(Object obj) { return obj instanceof C c && c.id == id; }
    }


    @ParameterizedTest @ValueSource(ints = {1, 2, 4, 8, 1<<10, 1<<20, 1<<31})
    void testExact(int capacity) {
        var pool = new ShallowLevelPool<>(C.class, 15);
        List<C> offers = new ArrayList<>(), gets = new ArrayList<>();
        for (int i = 0; i < 1+STEAL_ATTEMPTS; i++) offers.add(new C(capacity));
        for (int i = 0; i < 1+STEAL_ATTEMPTS; i++)
            assertNull(pool.offerExact(offers.get(i), capacity));
        C c = new C(capacity);
        assertSame(c, pool.offerExact(c, capacity), "poll should've rejected");

        for (int i = 0; i < 1 + STEAL_ATTEMPTS; i++)
             gets.add(pool.getAtLeast(capacity));
        assertEquals(new HashSet<>(offers), new HashSet<>(gets));
    }

    @Test
    void testFloor() {
        var pool = new ShallowLevelPool<>(C.class, 15);
        int offerCap = 24, getCap = 16, belowCap = 8;
        List<C> offers = new ArrayList<>(), got = new ArrayList<>();
        for (int i = 0; i < 2 * (1 + STEAL_ATTEMPTS); i++) offers.add(new C(offerCap));
        for (int i = 0; i < 2 * (1 + STEAL_ATTEMPTS); i++)
            assertNull(pool.offerToFloor(offers.get(i), offerCap));
        C c = new C(offerCap);
        assertSame(c, pool.offerToFloor(c, offerCap), "expected rejection");

        for (int i = 0; i < 1 + STEAL_ATTEMPTS; i++)
            got.add(pool.getAtLeast(getCap));
        assertNull(pool.getAtLeast(getCap));
        for (int i = 0; i < 1 + STEAL_ATTEMPTS; i++)
            got.add(pool.getAtLeast(belowCap));
        assertEquals(new HashSet<>(offers), new HashSet<>(got));
    }

    @Test
    void testOfferToNearest() {
        var pool = new ShallowLevelPool<>(C.class, 15);
        int offerCap = 24, getCap = 16;
        List<C> offers = new ArrayList<>(), gets = new ArrayList<>();
        for (int i = 0; i < 2 * (1 + STEAL_ATTEMPTS); i++) offers.add(new C(offerCap));
        for (int i = 0; i < 2 * (1 + STEAL_ATTEMPTS); i++)
            assertNull(pool.offerToNearest(offers.get(i), offerCap));
        List<C> ceilOffers = new ArrayList<>(), ceilGets = new ArrayList<>();
        for (int i = 0; i < 1 + STEAL_ATTEMPTS; i++) ceilOffers.add(new C(offerCap));
        for (int i = 0; i < 1 + STEAL_ATTEMPTS; i++)
            assertNull(pool.offerToNearest(ceilOffers.get(i), offerCap));

        C c = new C(offerCap);
        assertSame(c, pool.offerToNearest(c, offerCap));

        for (int i = 0; i < 1 + STEAL_ATTEMPTS; i++)
            ceilGets.add(pool.getAtLeast(getCap<<1));
        for (int i = 0; i < 2 * (1 + STEAL_ATTEMPTS); i++)
            gets.add(pool.getAtLeast(getCap>>1)); // will get from getCap once getcap>>1 is drained

        assertEquals(new HashSet<>(    offers), new HashSet<>(    gets));
        assertEquals(new HashSet<>(ceilOffers), new HashSet<>(ceilGets));
    }

    @Test
    void testConcurrent() throws Exception {
        for (int i = 0; i < 1_000; i++)
            doTestConcurrent();
    }

    private void doTestConcurrent() throws Exception {
        int threads = 1 << (32-numberOfLeadingZeros(getRuntime().availableProcessors()-1));
        int offerCap = 17, floorCap = 16, belowCap = 8, aboveCap = 32;
        var pool = new ShallowLevelPool<>(C.class, threads);
        List<C> offers = new ArrayList<>(), accepted = new ArrayList<>(), got = new ArrayList<>();
        for (int i = 0; i <  3 * threads; i++) {
            offers  .add(new C(offerCap));
            accepted.add(null);
        }
        int gotWidth = 3 + 3 * STEAL_ATTEMPTS;
        for (int i = 0; i < gotWidth*threads + 4*offers.size(); i++)
            got.add(null);

        try (var ex = Executors.newFixedThreadPool(threads)) {
            List<Future<?>> tasks = new ArrayList<>();
            //concurrently fill floorCap and belowCap
            var start0 = new Semaphore(0);
            var start1 = new AtomicBoolean(false);
            for (int threadIdx = 0; threadIdx < threads; threadIdx++) {
                int thread = threadIdx;
                tasks.add(ex.submit(() -> {
                    int in = thread*3, out = thread*gotWidth;
                    start0.acquireUninterruptibly();
                    while (!start1.get()) Thread.onSpinWait();
                    accepted.set(in  , pool.offerToNearest(offers.get(in  ), offerCap) == null ? offers.get(in  ) : null);
                    accepted.set(in+1, pool.offerToNearest(offers.get(in+1), offerCap) == null ? offers.get(in+1) : null);
                    accepted.set(in+2, pool.offerToNearest(offers.get(in+2), offerCap) == null ? offers.get(in+2) : null);
                    got.set(out++, pool.getAtLeast(aboveCap));
                    got.set(out++, pool.getAtLeast(floorCap));
                    got.set(out++, pool.getAtLeast(belowCap));
                    for (int i = 0; i < STEAL_ATTEMPTS; i++)
                        got.set(out++, pool.getAtLeast(aboveCap));
                    for (int i = 0; i < STEAL_ATTEMPTS; i++)
                        got.set(out++, pool.getAtLeast(floorCap));
                    for (int i = 0; i < STEAL_ATTEMPTS; i++)
                        got.set(out++, pool.getAtLeast(belowCap));
                }));
            }
            start0.release(threads);
            start1.set(true);
            for (Future<?> t : tasks) t.get();
            tasks.clear();
            assertTrue(IntStream.range(threads*gotWidth, got.size())
                                .allMatch(i -> got.get(i) == null), "got overflown");

            // drain the pool
            for (int i = 0; i < offers.size(); i++) {
                int out = threads*gotWidth + i*2;
                tasks.add(ex.submit(() -> got.set(out  , pool.getAtLeast(floorCap))));
                tasks.add(ex.submit(() -> got.set(out+1, pool.getAtLeast(belowCap))));
            }
            for (Future<?> task : tasks) task.get();

            List<C> acceptedNN = accepted.stream().filter(Objects::nonNull).toList();
            HashSet<C> acceptedSet = new HashSet<>(acceptedNN);
            assertEquals(acceptedNN.size(), acceptedSet.size(), "duplicate accepts");

            List<C> gotNN = got.stream().filter(Objects::nonNull).toList();
            HashSet<C> gotSet = new HashSet<>(gotNN);
            assertEquals(gotSet.size(), gotNN.size(), "duplicate gets");

            assertEquals(acceptedSet, gotSet);
        }
    }

}