package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.ChildJVM;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.WITH_STATE_AND_STATS;
import static java.lang.System.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.*;

class GatherAndScatterTest {
    private static final Vars X = Vars.of("x");
    private static final int THREADS = Runtime.getRuntime().availableProcessors();

    private static final class DummyException extends RuntimeException {
        public final int producer;
        public final long row;

        public DummyException(int producer, long row) {
            super("Producer "+producer+" failing at row "+row);
            this.producer = producer;
            this.row = row;
        }
    }

    private record FakeRoot(Collection<? extends StreamNode> nodes) implements StreamNode {
        @Override public Stream<? extends StreamNode> upstream() {return nodes.stream();}
    }

    record D(BatchType<?> batchType, int height, int nConsumers, int nProducers,
             int cancelAtRow, int failAtRow, int failingProducer)
            implements Runnable {

        public D(BatchType<?> batchType, int height, int nConsumers, int nProducers) {
            this(batchType, height, nConsumers, nProducers, -1, -1, -1);
        }

        private class P<B extends Batch<B>> extends TaskEmitter<B> {
            private final int id, end, failAt;
            private final ByteRope nt = new ByteRope();
            private int next;
            private boolean cleaned;

            public P(BatchType<B> batchType, Vars vars, EmitterService runner,
                     int id, int begin, int failAt) {
                super(batchType, vars, runner, RR_WORKER, CREATED, TASK_EMITTER_FLAGS);
                this.id     = id;
                this.next   = begin;
                this.end    = begin+height;
                this.failAt = failAt;
                if (ResultJournal.ENABLED)
                    ResultJournal.initEmitter(this, vars);
            }

            @Override public String label(StreamNodeDOT.Label type) {
                var sb = new StringBuilder().append("P(").append(id).append(')');
                if (type.showState())
                    sb.append("\nstate=").append(flags.render(state()));
                if (EmitterStats.ENABLED && type.showStats() && stats != null)
                    stats.appendToLabel(sb);
                return sb.toString();
            }

            @Override protected int produceAndDeliver(int state) {
                int rem = end - next;
                int limit = (int)Math.min(rem, (long)REQUESTED.getOpaque(this));
                int n = switch (id & 3) {
                    case 0  ->             Math.min( 1,               rem);
                    case 1  -> Math.max(1, Math.min( limit,           rem));
                    case 2  -> Math.max(1, Math.min( limit >> 2,      rem));
                    default -> Math.max(1, Math.min((limit >> 2) - 1, rem));
                };
                if (failAt >= next && failAt < next + n) {
                    throw new DummyException(id, next);
                } else {
                    B b = bt.createForThread(preferredWorker, 1);
                    for (long e = Math.min(end, next+n); next < e; next++) {
                        b.beginPut();
                        nt.clear().append('"').append(next);
                        b.putTerm(0, DT_integer, nt.u8(), 0, nt.len, true);
                        b.commitPut();
                    }
                    bt.recycleForThread(preferredWorker, deliver(b));
                }
                return next < end ? state|MUST_AWAKE : COMPLETED;
            }

            @Override public void rebind(BatchBinding binding) {
                throw new UnsupportedOperationException();
            }

            @Override protected void doRelease() {
                cleaned = true;
            }
        }

        private class C<B extends Batch<B>> implements Receiver<B> {
            private final AtomicReference<Thread> lock = new AtomicReference<>();
            private final ConsumerBarrier<B> barrier;
            private final Emitter<B> emitter;
            private final int id;
            private final EmitterStats stats = EmitterStats.createIfEnabled();
            private int state = Stateful.CREATED;
            private int[] history;
            private int historyLen = 0;

            private C(Emitter<B> emitter, ConsumerBarrier<B> barrier, int id) {
                emitter.subscribe(this);
                this.id = id;
                this.barrier = barrier;
                this.emitter = emitter;
                this.history = ArrayPool.intsAtLeast(8);
                Arrays.fill(this.history, 0);
            }

            @Override public Stream<? extends StreamNode> upstream() {
                return Stream.of(emitter);
            }

            public void release() {
                ArrayPool.INT.offer(history, history.length);
                history = ArrayPool.EMPTY_INT;
            }

            @Override public String toString() { return "C("+id+")"; }

            @Override public String label(StreamNodeDOT.Label type) {
                var sb = new StringBuilder().append("C(").append(id).append(')');
                if (type.showState())
                     sb.append("\nstate=").append(Stateful.Flags.DEFAULT.render(state));
                if (EmitterStats.ENABLED && type.showStats() && stats != null)
                    stats.appendToLabel(sb);
                return sb.toString();
            }

            private void lock() {
                Thread current = currentThread();
                assertNull(lock.compareAndExchangeAcquire(null, current),
                             "concurrent delivery");
            }
            private void unlock() {
                lock.setRelease(null);
            }

            @Override public @Nullable B onBatch(B batch) {
                if (EmitterStats.ENABLED && stats != null)
                    stats.onBatchReceived(batch);
                lock();
                try {
                    if (state == Stateful.CREATED)
                        state = Stateful.ACTIVE;
                    if (cancelAtRow >= 0 && historyLen + batch.totalRows() > cancelAtRow)
                        emitter.cancel();
                    assertNotNull(batch);
                    assertEquals(1, batch.cols);
                    int required = historyLen + batch.totalRows();
                    if (required > history.length) {
                        history = ArrayPool.grow(history, required);
                        Arrays.fill(history, historyLen, history.length, 0);
                    }
                    SegmentRope view = SegmentRope.pooled();
                    for (var node = batch; node != null; node = node.next) {
                        for (int r = 0, rows = node.rows; r < rows; r++) {
                            assertTrue(node.localView(r, 0, view));
                            history[historyLen++] = (int) view.parseLong(1);
                        }
                    }
                    view.recycle();
                } finally { unlock(); }
                return batch;
            }

            @Override public void onRow(B batch, int row) {
                if (EmitterStats.ENABLED && stats != null)
                    stats.onRowReceived();
                lock();
                try {
                    if (state == Stateful.CREATED)
                        state = Stateful.ACTIVE;
                    if (cancelAtRow >= 0 && historyLen  >= cancelAtRow)
                        emitter.cancel();
                    assertNotNull(batch);
                    assertTrue(row >= 0 && row < batch.rows);
                    assertEquals(1, batch.cols);
                    if (historyLen == history.length) {
                        history = ArrayPool.grow(history, historyLen + 1);
                        Arrays.fill(history, historyLen, history.length, 0);
                    }
                    SegmentRope view = SegmentRope.pooled();
                    assertTrue(batch.localView(row, 0, view));
                    history[historyLen++] = (int)view.parseLong(1);
                    view.recycle();
                } finally { unlock(); }
            }

            @Override public void onComplete() {
                lock();
                try {
                    state = Stateful.COMPLETED;
                    barrier.arrive(this, false, null);
                    state = Stateful.COMPLETED_DELIVERED;
                } finally { unlock(); }
            }

            @Override public void onCancelled() {
                lock();
                try {
                    state = Stateful.CANCELLED;
                    barrier.arrive(this, true, null);
                    state = Stateful.CANCELLED_DELIVERED;
                } finally { unlock(); }
            }

            @Override public void onError(Throwable cause) {
                lock();
                try {
                    state = Stateful.FAILED;
                    barrier.arrive(this, false, cause);
                    state = Stateful.FAILED_DELIVERED;
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
            ThreadJournal.resetJournals();
            ResultJournal.clear();
            //noinspection unchecked
            BatchType<B> batchType = (BatchType<B>) this.batchType;
            GatheringEmitter<B> gatherScatter = new GatheringEmitter<>(batchType, X);
            List<P<B>> producers = new ArrayList<>();
            for (int i = 0, begin = 0; i < nProducers; i++, begin += height) {
                int failAt = i == failingProducer ? begin+this.failAtRow : -1;
                P<B> p = new P<>(batchType, X, EMITTER_SVC, i, begin, failAt);
                gatherScatter.subscribeTo(p);
                producers.add(p);
            }
            Set<C<B>> consumers = new HashSet<>();
            FakeRoot fakeRoot = new FakeRoot(consumers);
            try {
                ConsumerBarrier<B> consumerBarrier = new ConsumerBarrier<>(consumers);
                for (int i = 0; i < nConsumers; i++)
                    consumers.add(new C<>(gatherScatter, consumerBarrier, i));
                long requestSize = (long) height * nProducers + 1;
                try (var w = ThreadJournal.watchdog(System.out, 100)) {
                    w.start(10_000_000_000L).andThen(() -> {
                        try {
//                            fakeRoot.renderDOT(new File("/tmp/test.svg"), WITH_STATE_AND_STATS);
                            System.out.println("\n");
                            ResultJournal.dump(System.out);
                            System.out.println("\n"+fakeRoot.toDOT(WITH_STATE_AND_STATS));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    if (requestSize > 32)
                        Batch.makeValidationCheaper();
                    gatherScatter.request(requestSize);
                    consumerBarrier.await();
                } finally {
                    Batch.restoreValidationCheaper();
                }
                assertTerminationStatus(consumerBarrier);
                assertHistory(consumers.iterator().next());
                assertSameHistory(consumers);
                assertCleanProducers(producers);
            } catch (Throwable t) {
                try {
                    ThreadJournal.dumpAndReset(System.out, 100);
                    ResultJournal.dump(System.out);
                    fakeRoot.renderDOT(new File("/tmp/test.svg"), WITH_STATE_AND_STATS);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                throw t;
            } finally {
                for (C<B> c : consumers) c.release();
            }
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
            long deadline = nanoTime()+10_000_000_000L;
            int clean = 0, nProducers = producers.size();
            while (clean != nProducers && nanoTime() < deadline)
                clean = (int)producers.stream().filter(p -> p.cleaned).count();
            assertEquals(nProducers, clean, "doRelease not called for some producers");
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
                assertEquals(expected, met, "consumer="+consumer);
            }
        }

        private <B extends Batch<B>> void assertSameHistory(Set<C<B>> consumers) {
            int[] ex = null;
            int exLen = 0;
            for (C<B> c : consumers) {
                if (ex == null) {
                    ex = c.history;
                    exLen = c.historyLen;
                } else {
                    assertEquals(exLen, c.historyLen, "history size differs");
                    int[] ac = c.history;
                    for (int i = 0; i < exLen; i++) {
                        if (ac[i] != ex[i])
                            fail("history mismatch at index "+i);
                    }
                }
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

    @AfterEach
    void tearDown() {
        Batch.restoreValidationCheaper();
    }

    @ParameterizedTest @MethodSource
    void test(D d) throws Exception {
        d.run();
        for (long s = Timestamp.nanoTime(); (Timestamp.nanoTime()-s) < 100_000_000L; )
            d.run();
        for (long s = Timestamp.nanoTime(); (Timestamp.nanoTime()-s) < 100_000_000L; )
            TestTaskSet.virtualRepeatAndWait(getClass().getSimpleName(), THREADS, d);
    }

    public static void main(String[] args) throws Exception {
        benchmark();
//        var ds = test().map(a -> (D)a.get()[0]).toList();
//        for (int i = 0; i < 10; i++) ds.get(i).run();
//        try (var rec = new Recording(Configuration.getConfiguration("profile"))) {
//            rec.setDumpOnExit(true);
//            rec.setDestination(Path.of("/tmp/profile.jfr"));
//            rec.start();
//            for (long t0 = nanoTime(); nanoTime()-t0 < 4_000_000_000L;) {
//                for (D d : ds) d.run();
//            }
//        }
    }

    public static void benchmark() throws Exception {
        double[] avgs = new double[5];
        for (int i = 0; i < avgs.length; i++) {
            long t0 = nanoTime();
            try (var jvm = ChildJVM.builder(BenchmarkHelper.class)
                    .jvmArgs(List.of("--enable-preview",
                            "--add-modules", "jdk.incubator.vector",
                            "-Dfastersparql.batch.pooled.mark=false",
                            "-Dfastersparql.batch.pooled.trace=false"))
                    .errorRedirect(ProcessBuilder.Redirect.INHERIT)
                    .outputRedirect(ProcessBuilder.Redirect.PIPE)
                    .build()) {
                String out = jvm.readAllOutput();
                var m = BenchmarkHelper.VAL_RX.matcher(out);
                if (!m.find())
                    throw new IOException("Unexpected child output"+out);
                avgs[i] = Double.parseDouble(m.group(1));
                System.out.printf("JVM %d: %6.3f ms\n", i, avgs[i]);
            }
            Async.uninterruptibleSleep(10+(int)((nanoTime()-t0)/1_000_000L));
        }
        double min = Double.MAX_VALUE, max = Double.MIN_VALUE, avg = 0, stdDev = 0;
        for (double v : avgs) {
            avg += v;
            if (v < min) min = v;
            if (v > max) max = v;
        }
        avg /= avgs.length;
        for (double v : avgs)
            stdDev += Math.pow(v-avg, 2);
        stdDev = Math.sqrt(stdDev/avgs.length);

        System.out.printf("between %6.3f and %6.3f ms, avg: %6.3f ± %2.3f ms\n",
                min, max, avg, stdDev);
    }

    public static class BenchmarkHelper {
        static final Pattern VAL_RX = Pattern.compile("GatherAndScatterTest-avg=(\\d+\\.\\d+)");
        public static void main(String[] args) {
            var ds = test().map(a -> (D)a.get()[0]).toList();
            for (long t0 = nanoTime(); (nanoTime()-t0) < 1_000_000_000L; ) {
                for (D d : ds) d.run();
            }
            System.gc();
            Async.uninterruptibleSleep(100);
            long t0 = nanoTime(), rounds = 0;
            for (; (nanoTime()-t0) < 4_000_000_000L; ++rounds) {
                for (D d : ds) d.run();
            }
            System.out.printf("GatherAndScatterTest-avg=%5.3f\n", (nanoTime()-t0)/1_000_000.0/rounds);
        }
    }

    @RepeatedTest(4)
    void testConcurrent() throws Exception {
        System.gc();
        Batch.makeValidationCheaper();
        String name = getClass().getSimpleName();
        try (TestTaskSet tasks = new TestTaskSet(name, newFixedThreadPool(THREADS))) {
            test().map(a -> (D) a.get()[0]).forEach(tasks::add);
        }
    }
}