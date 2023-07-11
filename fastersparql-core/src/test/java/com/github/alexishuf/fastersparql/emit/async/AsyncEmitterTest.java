package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.emit.async.RecurringTaskRunner.TASK_RUNNER;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.*;

class AsyncEmitterTest {
    private static final int THREADS = Runtime.getRuntime().availableProcessors();

    public static final class DummyException extends Exception {
        public final int producer;
        public final long row;

        public DummyException(int producer, long row) {
            super("Producer "+producer+" failing at row "+row);
            this.producer = producer;
            this.row = row;
        }
    }

    record D(BatchType<?> batchType, int height, int nConsumers, int nProducers,
             int cancelAtRow, int failAtRow, int failingProducer)
            implements Runnable {

        public D(BatchType<?> batchType, int height, int nConsumers, int nProducers) {
            this(batchType, height, nConsumers, nProducers, -1, -1, -1);
        }

        private class P<B extends Batch<B>> extends ProducerTask<B> {
            private final int id, end, failAt;
            private final ByteRope nt = new ByteRope();
            private int next;
            private boolean cleaned;

            public P(AsyncEmitter<B> emitter, RecurringTaskRunner runner,
                     int id, int begin, int failAt) {
                super(emitter, runner);
                this.id     = id;
                this.next   = begin;
                this.end    = begin+height;
                this.failAt = failAt;
                emitter.registerProducer(this);
            }

            @Override public String toString() { return "P("+id+")"; }

            @SuppressWarnings("unchecked") @Override
            protected @Nullable B produce(long limit, long deadline, @Nullable B b) throws Throwable {
                int rem = end - next;
                int n = switch (id & 3) {
                    case 0  ->                   Math.min( 1,               rem);
                    case 1  -> Math.max(1, (int) Math.min( limit,           rem));
                    case 2  -> Math.max(1, (int) Math.min( limit >> 2,      rem));
                    default -> Math.max(1, (int) Math.min((limit >> 2) - 1, rem));
                };
                if (failAt >= next && failAt < next + n) {
                    ((BatchType<B>) batchType).recycle(b);
                    throw new DummyException(id, next);
                } else {
                    if (b == null)   b = (B)batchType.create(n, 1, n*12);
                    else           { b.clear(); b.reserve(n, n*12); }
                    for (long e = Math.min(end, next+n); next < e; next++) {
                        b.beginPut();
                        nt.clear().append('"').append(next);
                        b.putTerm(0, DT_integer, nt.u8(), 0, nt.len, true);
                        b.commitPut();
                    }
                    return b;
                }
            }

            @Override protected boolean exhausted() {
                return next >= end;
            }

            @Override protected void cleanup(@Nullable Throwable reason) {
                cleaned = true;
            }
        }

        private class C<B extends Batch<B>> implements Receiver<B> {
            private final AtomicReference<Thread> lock = new AtomicReference<>();
            private final ConsumerBarrier<B> barrier;
            private final Emitter<B> emitter;
            private final int id;
            private int[] history = new int[128];
            private int historyLen = 0;

            private C(Emitter<B> emitter, ConsumerBarrier<B> barrier, int id) {
                emitter.subscribe(this);
                this.id = id;
                this.barrier = barrier;
                this.emitter = emitter;
            }

            @Override public String toString() { return "C("+id+")"; }

            private void lock() {
                Thread current = currentThread();
                assertNull(lock.compareAndExchangeAcquire(null, current),
                             "concurrent delivery");
            }
            private void unlock() {
                lock.setRelease(null);
            }

            @Override public @Nullable B onBatch(B batch) {
                lock();
                try {
                    if (cancelAtRow >= 0 && historyLen + batch.rows > cancelAtRow)
                        emitter.cancel();
                    assertNotNull(batch);
                    int required = historyLen + batch.rows;
                    if (required > history.length)
                        history = Arrays.copyOf(history, Math.max(required, history.length << 1));
                    assertEquals(1, batch.cols);
                    SegmentRope view = SegmentRope.pooled();
                    for (int r = 0, rows = batch.rows; r < rows; r++) {
                        assertTrue(batch.localView(r, 0, view));
                        history[historyLen++] = (int) view.parseLong(1);
                    }
                    view.recycle();
                } finally { unlock(); }
                return batch;
            }

            @Override public void onComplete() {
                lock();
                try {
                    barrier.arrive(this, false, null);
                } finally { unlock(); }
            }

            @Override public void onCancelled() {
                lock();
                try {
                    barrier.arrive(this, true, null);
                } finally { unlock(); }
            }

            @Override public void onError(Throwable cause) {
                lock();
                try {
                    barrier.arrive(this, false, cause);
                } finally { unlock(); }
            }
        }

        private final class ConsumerBarrier<B extends Batch<B>> {
            private final Lock lock = new ReentrantLock();
            private final Condition ready = lock.newCondition();
            private final Set<C<B>> readyConsumers = new HashSet<>();
            private final Set<C<B>> expectedConsumers;
            public boolean cancelled;
            public @Nullable Throwable error;

            public ConsumerBarrier(Set<C<B>> expectedConsumers) {
                this.expectedConsumers = expectedConsumers;
            }

            public void arrive(C<B> consumer, boolean cancelled, @Nullable Throwable error) {
                lock.lock();
                try {
                    assertTrue(expectedConsumers.contains(consumer), "unexpected "+consumer);
                    if (readyConsumers.add(consumer)) {
                        if (cancelled)     this.cancelled = true;
                        if (error != null) this.error     = error;
                        ready.signalAll();
                    }
                } finally {lock.unlock();}
            }

            public void await() {
                lock.lock();
                try {
                    while (readyConsumers.size() < expectedConsumers.size())
                        ready.awaitUninterruptibly();
                } finally {lock.unlock();}
            }
        }

        @Override public void run() { genericRun(); }
        private <B extends Batch<B>> void genericRun() {
            //noinspection unchecked
            var ae = new AsyncEmitter<>((BatchType<B>) batchType, Vars.of("x"), TASK_RUNNER);
            List<P<B>> producers = new ArrayList<>();
            for (int i = 0, begin = 0; i < nProducers; i++, begin += height) {
                int failAt = i == failingProducer ? begin+this.failAtRow : -1;
                producers.add(new P<>(ae, TASK_RUNNER, i, begin, failAt));
            }
            Set<C<B>> consumers = new HashSet<>();
            ConsumerBarrier<B> consumerBarrier = new ConsumerBarrier<>(consumers);
            for (int i = 0; i < nConsumers; i++)
                consumers.add(new C<>(ae, consumerBarrier, i));
            long requestSize = (long) height * nProducers + 1;
            boolean oldDisableValidate = CompressedBatch.DISABLE_VALIDATE;
            try {
                CompressedBatch.DISABLE_VALIDATE = requestSize > 1_024;
                ae.request(requestSize);
                consumerBarrier.await();
            } finally {
                CompressedBatch.DISABLE_VALIDATE = oldDisableValidate;
            }
            assertTerminationStatus(consumerBarrier);
            assertCleanProducers(producers);
            assertHistory(consumers.iterator().next());
            assertSameHistory(consumers);
        }

        private <B extends Batch<B>> void assertTerminationStatus(ConsumerBarrier<B> barrier) {
            if (failAtRow >= 0 && failingProducer >= 0) {
                assertNotNull(barrier.error);
                DummyException dummy = barrier.error instanceof DummyException e ? e : null;
                if (dummy == null) {
                    for (Throwable t : barrier.error.getSuppressed()) {
                        if (t instanceof DummyException e) {
                            dummy = e;
                            break;
                        }
                    }
                }
                assertNotNull(dummy, "Expected DummyException, got "+ barrier.error);
                assertEquals(failingProducer, dummy.producer);
                assertEquals(failAtRow+((long)failingProducer*height), dummy.row);
            } else if (barrier.error != null) {
                fail("Unexpected error", barrier.error);
            } else if (barrier.cancelled) {
                assertTrue(cancelAtRow >= 0, "unexpected cancel result");
            }
        }

        private <B extends Batch<B>> void assertCleanProducers(List<P<B>> producers) {
            for (int i = 0; i < producers.size(); i++)
                assertTrue(producers.get(i).cleaned, i+"-th producer not clean for "+this);
        }

        private <B extends Batch<B>> void assertHistory(C<B> consumer) {
            int bound = nProducers * height;
            BitSet met = new BitSet(bound), expected = new BitSet();
            int[] hist = consumer.history;
            for (int i = 0, len = consumer.historyLen; i < len; i++) {
                int value = hist[i];
                if (value < 0 || value >= bound)
                    fail("value "+value+" not in [0, "+bound+")");
                if (met.get(value))
                    fail("value "+value+" is duplicate");
                met.set(value);
            }
            expected.set(0, bound);
            if (failingProducer < nProducers && failAtRow >= 0) {
                int begin = failingProducer*height, end = begin+height;
                expected.clear(begin, end);
                met.clear(begin, end);
            }
            if (cancelAtRow >= 0) {
                assertTrue(met.cardinality() >= cancelAtRow,
                           "cancelAtRow="+cancelAtRow+", but got only "+met.cardinality()+" rows");
            } else {
                assertEquals(expected, met);
            }
        }

        private <B extends Batch<B>> void assertSameHistory(Set<C<B>> consumers) {
            int[] ex = null;
            for (C<B> c : consumers) {
                if (ex == null) ex = c.history;
                else            assertArrayEquals(ex, c.history);
            }
        }
    }

    static Stream<Arguments> test() {
        List<D> list = new ArrayList<>(List.of(
                // single producer, single consumer
                new D(TERM, 1, 1, 1),
                new D(TERM, 2, 1, 1),
                new D(TERM, 15, 1, 1),
                new D(TERM, 63, 1, 1),
                new D(TERM, 64, 1, 1),
                new D(TERM, 65, 1, 1),
                new D(TERM, 127, 1, 1),
                new D(TERM, 128, 1, 1),
                new D(TERM, 129, 1, 1),
                new D(TERM, 8191, 1, 1),
                new D(TERM, 8192, 1, 1),
                new D(TERM, 8193, 1, 1),

                // shallow successful gather
                new D(TERM, 1, 1,   2),
                new D(TERM, 1, 1,   4),
                new D(TERM, 1, 1, 128),
                new D(TERM, 1, 1, 256),

                // shallow successful scatter
                new D(TERM, 1, 2,   1),
                new D(TERM, 1, 4,   1),
                new D(TERM, 1, 128, 1),
                new D(TERM, 1, 256, 1),

                //shallow successful scatter and gather
                new D(TERM, 1, 2, 2),
                new D(TERM, 1, 4, 4),
                new D(TERM, 1, 31, 33),

                // deep successful gather
                new D(TERM, 8191, 1,   2),
                new D(TERM, 8192, 1,   4),
                new D(TERM, 8193, 1, 64),

                // deep successful scatter
                new D(TERM, 8191, 2,   1),
                new D(TERM, 8192, 4,   1),
                new D(TERM, 8193, 64, 1),

                // deep successful scatter and gather
                new D(TERM, 256, 2,   2),
                new D(TERM, 256, 4,   4),
                new D(TERM, 256, 33, 31),

                // shallow with producer failing early
                new D(TERM, 1, 1,   1, -1, 0, 0),
                new D(TERM, 1, 1,  64, -1, 0, 0),
                new D(TERM, 1, 64,  1, -1, 0, 0),
                new D(TERM, 1, 64, 64, -1, 0, 0),

                // deep with producer failing early
                new D(TERM, 256, 1,   1, -1, 0, 0),
                new D(TERM, 256, 1,  64, -1, 0, 32),
                new D(TERM, 256, 64,  1, -1, 0, 0),
                new D(TERM, 256, 64, 64, -1, 0, 0),

                // deep with producer failing late
                new D(TERM, 256, 1,  1, -1, 100, 0),
                new D(TERM, 256, 1, 64, -1, 100, 16),
                new D(TERM, 256, 64, 1, -1, 100, 0),
                new D(TERM, 256, 8,  8, -1, 100, 0),

                // shallow with cancel early
                new D(TERM, 1,   1,   1, 0, -1, -1),
                new D(TERM, 1,   1, 128, 0, -1, -1),
                new D(TERM, 1, 128,   1, 0, -1, -1),
                new D(TERM, 1, 128, 128, 0, -1, -1),

                // deep with cancel early
                new D(TERM, 256,   1,   1, 0, -1, -1),
                new D(TERM, 256,   1, 128, 0, -1, -1),
                new D(TERM, 256, 128,   1, 0, -1, -1),
                new D(TERM, 256, 128, 128, 0, -1, -1),

                // deep with cancel late
                new D(TERM, 256,   1,  1, 100, -1, -1),
                new D(TERM, 256,   1, 64, 100, -1, -1),
                new D(TERM, 256, 64,   1, 100, -1, -1),
                new D(TERM, 256, 8,    8, 100, -1, -1)
        ));
        for (int i = 0, n = list.size(); i < n; i++) {
            D d = list.get(i);
            list.add(new D(COMPRESSED, d.height, d.nConsumers, d.nProducers, d.cancelAtRow, d.failAtRow, d.failingProducer));
        }
        return list.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(D d) throws Exception {
        d.run();
        TestTaskSet.virtualRepeatAndWait(getClass().getSimpleName(), THREADS, d);
}

    @Test
    void testConcurrent() throws Exception {
        System.gc();
        boolean disableValidate = CompressedBatch.DISABLE_VALIDATE;
        CompressedBatch.DISABLE_VALIDATE = true;
        String name = getClass().getSimpleName();
        try (TestTaskSet tasks = new TestTaskSet(name, newFixedThreadPool(THREADS))) {
            test().map(a -> (D) a.get()[0])
                    .forEach(tasks::add);
        } finally {
            CompressedBatch.DISABLE_VALIDATE = disableValidate;
        }
    }


}