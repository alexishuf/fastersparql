package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.util.concurrent.LevelAlloc.Capacities;
import org.junit.jupiter.api.RepeatedTest;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

class LevelAllocTest {
    private static final AtomicInteger nextId = new AtomicInteger();

    public static final class C {
        private static final VarHandle LOCK;
        static {
            try {
                LOCK = MethodHandles.lookup().findVarHandle(C.class, "locked", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        public final int id;
        public final int len;
        @SuppressWarnings("unused") private int locked;

        public C(int id, int len) {
            this.id = id;
            this.len = len;
        }

        public C(int len) {
            this(nextId.getAndIncrement(), len);
        }

        public void lock() {
            assertEquals(0, (int)LOCK.compareAndExchangeAcquire(this, 0, 1));
        }
        public void unlock() {
            assertEquals(1, (int)LOCK.compareAndExchangeRelease(this, 1, 0));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (C) obj;
            return this.id == that.id && this.len == that.len;
        }

        @Override public int hashCode() {return Objects.hash(id, len);}

        @Override public String toString() {return "C[id=" + id + ", len=" + len + ']';}

        }
    public static final IntFunction<C> C_FAC = new IntFunction<>() {
        @Override public C apply(int len) {return new C(len);}
        @Override public String toString() {return "C_FAC";}
    };

    @RepeatedTest(2) void test() {
        var alloc = new LevelAlloc<>(C.class, "alloc", 24, 1,
                                     C_FAC, new Capacities().set(0, 4, 7));
        for (int level = 0; level <= 10; level++)
            alloc.primeLevel(level);
        for (int level = 0; level <= 4; level++) {
            for (int i = 0; i < 7; i++) {
                C c = (i&1) == 0 ? alloc.pollAtLeast(1<<level) : alloc.pollFromLevel(level);
                assertNotNull(c);
                assertEquals(1<<level, c.len);
            }
            assertNull((level&1) == 0 ? alloc.pollAtLeast(1<<level) : alloc.pollFromLevel(level));
            C c = (level&1) == 0 ? alloc.createFromLevel(level)
                                 : alloc.createAtLeast(1<<level);
            assertNotNull(c);
            assertEquals(1<<level, c.len);
        }

        C c = alloc.createFromLevel(4);
        assertNotNull(c);
        assertEquals(16, c.len);
        for (int thread = 0; thread < Alloc.THREADS; thread++)
            assertNull(alloc.pollFromLevel(thread, 4));

        for (int thread = 0; thread < Alloc.THREADS; thread++) {
            assertNotNull(c = alloc.createAtLeast(thread, 16));
            assertEquals(16, c.len);
            if ((thread & 1) == 0)
                assertNull(alloc.offerToLevel(thread, c, 4));
            else
                assertNull(alloc.offer(thread, c, 16));
            if ((thread&1) == 0)
                assertSame(c, alloc.pollAtLeast(thread+1, 16));
            else
                assertSame(c, alloc.pollFromLevel(thread+1, 4));
        }
    }


    @RepeatedTest(4)
    void testConcurrent() throws Exception {
        Capacities capacities = new Capacities().set(0, 4, Alloc.THREADS*32);
        var alloc = new LevelAlloc<>(C.class, "alloc", 24, 1,
                                     C_FAC, capacities);
        int beginId = nextId.get();
        alloc.primeLevel(4, 2, 0);
        int primedId = nextId.get();
        assertEquals(Alloc.THREADS*16, primedId-beginId);
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            tasks.repeat(Alloc.THREADS, thread -> {
                int n = thread < 7 ? 8 : 7;
                C[] tmp = new C[n];
                for (int round = 0; round < 200_000; round++) {
                    for (int i = 0; i < n; i++) {
                        tmp[i] = alloc.createFromLevel(4);
                        tmp[i].lock();
                    }
                    Thread.yield();
                    for (int i = 0; i < n; i++) {
                        tmp[i].unlock();
                        alloc.offer(tmp[i], 16);
                    }
                }
            });
        }
        assertEquals(primedId, nextId.get());

    }
}