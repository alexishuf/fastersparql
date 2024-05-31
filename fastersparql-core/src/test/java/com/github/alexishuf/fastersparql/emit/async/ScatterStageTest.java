package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.CollectingReceiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.concurrent.Watchdog;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.util.concurrent.Timestamp.nanoTime;
import static org.junit.jupiter.api.Assertions.*;

class ScatterStageTest {
    private static final Vars XY = Vars.of("x", "y");
    private static final int MAX_HEIGHT = 1024;
    private static final Term[] INTEGERS, URIS;

    static {
        INTEGERS = new Term[MAX_HEIGHT];
        URIS     = new Term[MAX_HEIGHT];
        for (int i = 0; i < MAX_HEIGHT; i++) {
            INTEGERS[i] = Term.array(i)[0];
            URIS    [i] = Term.valueOf("<http://example.org/integers#"+i+">");
        }
    }

    @BeforeAll static void beforeAll() { Batch.makeValidationCheaper(); }
    @AfterAll  static void  afterAll() { Batch.restoreValidationCheaper(); }

    private static final class P extends TaskEmitter<CompressedBatch, P> implements Orphan<P> {
        private @Nullable CompressedBatch current;
        private final boolean injectCancel;
        private final @Nullable RuntimeException injectFail;
        private int absRow, relRow;
        private final int totalRows;

        public P(Orphan<CompressedBatch> expected, boolean injectCancel,
                 @Nullable RuntimeException injectFail) {
            super(COMPRESSED, XY, CREATED, TASK_FLAGS);
            this.current = expected.takeOwnership(this);
            assert current.validate(Batch.Validation.CHEAP);
            this.totalRows = current.totalRows();
            this.injectCancel = injectCancel;
            this.injectFail = injectFail;
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
        }

        @Override protected void doRelease() {
            current = Owned.recycle(current, this);
            super.doRelease();
        }

        @Override public P takeOwnership(Object o) {return takeOwnership0(o);}

        @Override public void rebind(BatchBinding binding) throws RebindException {
            throw new UnsupportedOperationException();
        }

        @Override public Vars bindableVars() { return Vars.EMPTY; }

        @Override protected int produceAndDeliver(int state) {
            if (current != null && relRow >= current.rows) {
                current = current.next;
                relRow = 0;
                assert current == null || current.rows > 0;
            }
            if (absRow >= totalRows) {
                if      (injectFail != null) throw injectFail;
                else if (injectCancel)       return CANCELLED;
                else                         return COMPLETED;
            }
            assert current != null;
            deliver(current.dupRow(relRow, threadId));
            ++absRow;
            ++relRow;
            return state;
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() { return Stream.of(); }
    }

    record D(int consumersCount, int height, @Nullable RuntimeException error, boolean cancel)
            implements Runnable {
        public void run() {
            List<CollectingReceiver<CompressedBatch>> receivers = new ArrayList<>();
            ScatterStage<CompressedBatch> scatter;
            P producer;
            try (var expectedG = new Guard.BatchGuard<CompressedBatch>(this)) {
                var expected = expectedG.set(makeExpected());
                producer = new P(makeExpected(), cancel, error);
                scatter = new ScatterStage<>(producer);
                for (int i = 0; i < consumersCount; i++)
                    receivers.add(CollectingReceiver.create(scatter.createConnector()).takeOwnership(this));
                for (int i = 0; i < consumersCount; i++)
                    receivers.get(i).start();

                for (int i = 0; i < consumersCount; i++) {
                    var rcv = receivers.get(i);
                    try {
                        CompressedBatch actual = rcv.get();
                        assertEquals(expected, actual, "consumer="+i);
                        if (error != null) fail("Expected onError(" + error + "), consumer="+i);
                        if (cancel) fail("Expected onCancelled(), consumer="+i);
                    } catch (InterruptedException e) {
                        fail(e);
                    } catch (ExecutionException e) {
                        if (error == null && !cancel)
                            fail("Unexpected error", e);
                        if (error != null)
                            assertSame(error, e.getCause());
                        if (cancel)
                            assertInstanceOf(FSCancelledException.class, e.getCause());
                    }
                }
            } finally {
                for (CollectingReceiver<CompressedBatch> r : receivers)
                    Owned.safeRecycle(r, this);
                // recycling of receivers must cause recycling of scatter
            }
            // recycling of receivers must cause recycling of scatter
            assertFalse(scatter.isAliveAndMarking());
            for (long d = nanoTime()+1_000_000_000; producer.isAliveAndMarking() && nanoTime() < d; )
                Thread.yield();
            assertFalse(producer.isAliveAndMarking());
        }

        private Orphan<CompressedBatch> makeExpected() {
            var expected = COMPRESSED.create(2).takeOwnership(this);
            for (int r = 0; r < height; r++) {
                expected.beginPut();
                expected.putTerm(0, INTEGERS[r]);
                expected.putTerm(1, URIS[r]);
                expected.commitPut();
            }
            return expected.releaseOwnership(this);
        }
    }

     static Stream<Arguments> test() {
        RuntimeException fail = new RuntimeException("test");
        return Stream.of(
                new D(1, 1, null, false),
                new D(1, 1, fail, false),
                new D(1, 1, null, true),

                new D(1, 8, null, false),
                new D(1, 8, fail, false),
                new D(1, 8, null, true),

                new D(1, MAX_HEIGHT, null, false),
                new D(1, MAX_HEIGHT, fail, false),
                new D(1, MAX_HEIGHT, null, true),

                new D(8, 1, null, false),
                new D(8, 1, fail, false),
                new D(8, 1, null, true),

                new D(8, MAX_HEIGHT, null, false),
                new D(8, MAX_HEIGHT, fail, false),
                new D(8, MAX_HEIGHT, null, true)
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(D d) {
        for (int i = 0; i < 200; i++) {
            Watchdog.reset();
            try (var w = Watchdog.spec("test").threadStdOut(100).create()) {
                w.start(2_000_000_000L);
                d.run();
            } catch ( Throwable t) {
                ThreadJournal.dumpAndReset(System.out, 100);
                fail(t);
            }
        }
    }

    @Test
    public void testConcurrent() throws Exception {
        test(new D(1, 8, null, false));
        int threads = Runtime.getRuntime().availableProcessors();
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            // repeat(threads, d) is intentional, there will be threads**2 d.run()s
            test().map(a -> (D)a.get()[0])
                    .forEach(d -> tasks.repeat(threads, d));
        }
    }


}