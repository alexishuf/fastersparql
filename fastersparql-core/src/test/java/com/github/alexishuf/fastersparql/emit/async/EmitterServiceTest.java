package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.Watchdog;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Runtime.getRuntime;
import static org.junit.jupiter.api.Assertions.*;

class EmitterServiceTest {
    private static final int REQUEST_SIZE = 1_000;
    private final AtomicLong shared = new AtomicLong();
    private final Semaphore countersStopped = new Semaphore(0);
    private final ConcurrentLinkedDeque<Throwable> errors = new ConcurrentLinkedDeque<>();

    private static abstract class Counter extends EmitterService.Task<Counter> {
        private final EmitterServiceTest test;
        private final AtomicBoolean running = new AtomicBoolean();
        public int local;
        public int requested;

        protected Counter(EmitterServiceTest test) {
            super(CREATED, TASK_FLAGS);
            this.test = test;
        }
        public static Orphan<Counter> create(EmitterServiceTest test) {
            return new Concrete(test);
        }
        private static final class Concrete extends Counter implements Orphan<Counter> {
            public Concrete(EmitterServiceTest test) {super(test);}
            @Override public Counter takeOwnership(Object o) {return takeOwnership0(o);}
        }

        public void request() {
            requested = REQUEST_SIZE;
            awakeSameWorker();
        }

        @Override protected void task(EmitterService.@Nullable Worker worker, int threadId) {
            try {
                assertNotNull(worker);
                assertEquals(threadId, worker.threadId, "bad threadId");
                assertEquals(threadId, (int)worker.threadId(), "threadId != (int)threadId()");
                assertEquals(Thread.currentThread(), worker, "worker is not currentThread()");
                assertEquals(0, statePlain()&RELEASED_MASK, "task() after release");
                assertFalse(running.getAndSet(false));
                ++local;
                test.shared.getAndIncrement();
                if (--requested > 0)
                    awakeSameWorker(worker);
                else if (requested == 0)
                    test.countersStopped.release();
                else
                    fail(this+".task() without previous awake()");
            } catch (Throwable t) {
                test.errors.add(t);
                test.countersStopped.release();
                throw t;
            }
        }
    }

    @ParameterizedTest @ValueSource(doubles = {0, 0.5, 1, 1.5, 2, 8})
    public void test(double multiplier) {
        int processors = getRuntime().availableProcessors();
        var tasks = new Counter[(int)Math.max(1, processors*multiplier)];
        for (int i = 0; i < tasks.length; i++)
            tasks[i] = Counter.create(this).takeOwnership(this);
        try (var w = Watchdog.spec("test").threadStdOut(100).emitterServiceStdOut().startSecs(10)) {
            Async.uninterruptibleSleep(100);
            assertTrue(errors.isEmpty());
            assertEquals(0, shared.get());
            for (Counter task : tasks) assertEquals(0, task.local);

            for (int i = 0; i < 200; i++) {
                errors.clear();
                shared.setRelease(0);
                for (var t : tasks) {
                    t.local = 0;
                    t.request();
                }
                countersStopped.acquireUninterruptibly(tasks.length);
                assertEquals((long) tasks.length * REQUEST_SIZE, shared.get());
                for (Counter task : tasks)
                    assertEquals(REQUEST_SIZE, task.local);
                if (!errors.isEmpty()) {
                    var sb = new StringBuilder();
                    for (Throwable e : errors)
                        sb.append(e).append('\n');
                    assertEquals("", sb.toString());
                }
            }
            w.stop();
            shared.setRelease(0);
        } finally {
            for (Counter t : tasks)
                Owned.recycle(t, this);
        }
        Async.uninterruptibleSleep(100);
        assertEquals(0, shared.get(), "task() executions after recycle()");
    }

}