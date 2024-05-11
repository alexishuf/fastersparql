package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.emit.async.Stateful.*;
import static org.junit.jupiter.api.Assertions.*;

class StatefulTest {

    private static final int F8    = 0x80000000;
    private static final int F4    = 0x40000000;
    private static final int F_LOW = 0x00010000;
    private static final Stateful.Flags FLAGS = Stateful.Flags.DEFAULT.toBuilder()
            .flag(F8, "F8")
            .flag(F4, "F4")
            .flag(F_LOW, "F_LOW")
            .build();

    private static abstract class S extends Stateful<S> {
        public S() {super(CREATED, FLAGS);}
        public static Orphan<S> create() { return new Concrete(); }
        private static final class Concrete extends S implements Orphan<S> {
            @Override public StatefulTest.S takeOwnership(Object o) {return takeOwnership0(o);}
        }
    }

    @Test void testLockSingleThread() {
        try (var sGuard = new Guard<S>(this)) {
            S s = sGuard.set(S.create());
            assertTrue(s.moveStateRelease(s.state(), ACTIVE), "CREATED -> ACTIVE is a legal move");
            assertEquals(ACTIVE|F8, s.setFlagsRelease(F8));

            assertEquals(ACTIVE|F8|LOCKED_MASK, s.lock(),
                    "lock(st) must return with LOCKED bit set");
            assertEquals(ACTIVE|F8,             s.unlock(),
                    "unlock() must returns with LOCKED bit unset");

            assertThrows(IllegalStateException.class, s::unlock,
                    "unlock() unlocked must fail");
            assertThrows(IllegalStateException.class, s::unlock,
                    "unlock() unlocked must fail");

            assertEquals(ACTIVE|F8|LOCKED_MASK, s.lock(), "lock() must tolerate wrong hint");
            assertEquals(ACTIVE|F8,             s.unlock(), "unlock() must tolerate wrong hint");

            assertEquals(ACTIVE|F8|LOCKED_MASK, s.lock());
            assertEquals(ACTIVE|F4,             s.unlock(F8, F4));
            assertEquals(ACTIVE,                s.clearFlagsAcquire(F4));
            assertEquals(ACTIVE|F_LOW,          s.setFlagsRelease(F_LOW));

            assertEquals(ACTIVE|F_LOW|LOCKED_MASK, s.lock(), "failed to lock");
            assertEquals(ACTIVE|F_LOW,             s.unlock(),
                    "second unlock must unlock");
        }
    }

    @Test void testMoveState() {
        try (var sGuard = new Guard<S>(this)) {
            S s = sGuard.set(S.create());
            assertTrue(s.moveStateRelease(CREATED, ACTIVE));
            assertFalse(s.moveStateRelease(ACTIVE, CREATED));
            assertTrue(s.compareAndSetFlagAcquire(F8));
            assertFalse(s.compareAndSetFlagRelease(F8));
            assertEquals(ACTIVE|F8|LOCKED_MASK, s.lock());
            assertEquals(ACTIVE|F8, s.unlock());
            assertTrue(s.moveStateRelease(CREATED, COMPLETED));
            assertTrue(s.markDelivered(COMPLETED));
        }
    }

    @Test void testMoveStateRace() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors();
        S[] objs = new S[200_000];
        try {
            for (int i = 0; i < objs.length; i++)
                objs[i] = S.create().takeOwnership(this);
            AtomicInteger index = new AtomicInteger(0);
            try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
                tasks.repeat(threads, thread -> {
                    for (int i = 0; i < objs.length; i++) {
                        while (index.getOpaque() < i)
                            Thread.onSpinWait();
                        StatefulTest.S s = objs[i];
                        s.moveStateRelease(CREATED, ACTIVE);
                        int st = s.lock();
                        assertEquals(LOCKED_MASK, st&LOCKED_MASK);
                        s.setFlagsRelease(F8);
                        s.compareAndSetFlagAcquire(F4);
                        s.clearFlagsAcquire(F8);
                        assertEquals(0, (s.unlock()&LOCKED_MASK));
                        s.moveStateRelease(ACTIVE, COMPLETED);
                        s.markDelivered(COMPLETED);
                        if (thread == 0)
                            index.incrementAndGet();
                    }
                });
            }
        } finally {
            for (var s : objs) {
                assertTrue((s.state()&IS_TERM_DELIVERED) != 0);
                s.recycle(this);
            }
        }
    }

    private static abstract class RaceTask extends EmitterService.Task<RaceTask> {
        private final S s;
        private final int rounds;
        private final int[] counter;
        private final CompletableFuture<?> result = new CompletableFuture<>();
        private RaceTask(S s, int rounds, int[] counter) {
            super(EmitterService.EMITTER_SVC, RR_WORKER, CREATED, TASK_FLAGS);
            this.s       = s;
            this.rounds  = rounds;
            this.counter = counter;
        }

        public static RaceTask create(S s, int rounds, int[] counter) {
            return new Concrete(s, rounds, counter);
        }

        private static final class Concrete extends RaceTask implements Orphan<RaceTask> {
            public Concrete(S s, int rounds, int[] counter) {super(s, rounds, counter);}
            @Override public RaceTask takeOwnership(Object o) {return takeOwnership0(o);}
        }

        public void await() {
            boolean interrupted = false;
            while (true) {
                try {
                    result.get();
                    if (interrupted)
                        Thread.currentThread().interrupt();
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (ExecutionException e) {
                    Assertions.fail(e);
                }
            }
        }

        @Override protected void task(int threadId) {
            try {
                for (int i = 0; i < rounds; i++) {
                    s.lock();
                    counter[0]++;
                    if ((i&1) == 0) {
                        int clear = (i&1) == 0 ? F4 : F8;
                        int set   = (i&1) == 0 ? F8 : F4;
                        s.unlock(clear, set);
                    } else {
                        s.unlock();
                    }
                }
                result.complete(null);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        }
    }

    @Test void testRace() {
        int[] counter = {0};
        int threads = Runtime.getRuntime().availableProcessors()*10;
        int rounds = 100_000;
        RaceTask[] tasks = new RaceTask[threads];
        try (var sGuard = new Guard<S>(this)) {
            var s = sGuard.set(S.create());
            for (int i = 0; i < tasks.length; i++)
                tasks[i] = RaceTask.create(s, rounds, counter);
            for (RaceTask task : tasks)
                task.awake();
            for (RaceTask task : tasks)
                task.await();
            int st = s.lock();
            assertTrue((st&(F4|F8)) != 0, "neither F4 nor F8 are set");
            assertEquals(threads*rounds, counter[0]);
            assertEquals(st&~LOCKED_MASK, s.unlock(), "unlock() must unset LOCKED bit");
        }
    }
}