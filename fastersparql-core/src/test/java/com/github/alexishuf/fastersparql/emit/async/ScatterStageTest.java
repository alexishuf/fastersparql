package com.github.alexishuf.fastersparql.emit.async;

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

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static org.junit.jupiter.api.Assertions.*;

class ScatterStageTest {
    private static final Vars XY = Vars.of("x", "y");
    private static final int MAX_HEIGHT = 1024;
    private static final Term[] INTEGERS, URIS;
    private static boolean oldDisableValidate;

    static {
        INTEGERS = new Term[MAX_HEIGHT];
        URIS     = new Term[MAX_HEIGHT];
        for (int i = 0; i < MAX_HEIGHT; i++) {
            INTEGERS[i] = Term.array(i)[0];
            URIS    [i] = Term.valueOf("<http://example.org/integers#"+i+">");
        }
    }

    @BeforeAll static void beforeAll() {
        oldDisableValidate = CompressedBatch.DISABLE_VALIDATE;
        CompressedBatch.DISABLE_VALIDATE = true;
    }

    @AfterAll static void afterAll() {
        CompressedBatch.DISABLE_VALIDATE = oldDisableValidate;
    }

    private static final class P extends TaskEmitter<CompressedBatch> {
        private final CompressedBatch expected;
        private final boolean injectCancel;
        private final @Nullable RuntimeException injectFail;
        private int row;

        public P(CompressedBatch expected, boolean injectCancel,
                 @Nullable RuntimeException injectFail) {
            super(COMPRESSED, XY, EMITTER_SVC, RR_WORKER, CREATED, TASK_EMITTER_FLAGS);
            this.expected = expected;
            this.injectCancel = injectCancel;
            this.injectFail = injectFail;
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            throw new UnsupportedOperationException();
        }

        @Override protected int produceAndDeliver(int state) {
            if (row >= expected.rows) {
                if      (injectFail != null) throw injectFail;
                else if (injectCancel)       return CANCELLED;
                else                         return COMPLETED;
            }
            if ((row&1) == 0) {
                deliverRow(expected, row);
            } else {
                var b = COMPRESSED.create(1, expected.cols, 0);
                b.putRow(expected, row);
                COMPRESSED.recycle(deliver(b));
            }
            ++row;
            return state|MUST_AWAKE;
        }

        @Override public Stream<? extends StreamNode> upstream() { return Stream.of(); }
    }

    record D(int consumersCount, int height, @Nullable RuntimeException error, boolean cancel)
            implements Runnable {
        public void run() {
            var expected = makeExpected();
            var scatter = new ScatterStage<>(COMPRESSED, XY);
            P producer = new P(expected, cancel, error);
            scatter.subscribeTo(producer);
            List<CollectingReceiver<CompressedBatch>> receivers = new ArrayList<>();
            for (int i = 0; i < consumersCount; i++) {
                var r = new CollectingReceiver<>(scatter);
                receivers.add(r);
            }
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
                        assertTrue(e.getCause() instanceof FSCancelledException);
                }
            }
        }

        private CompressedBatch makeExpected() {
            var expected = COMPRESSED.create(height, 2, 0);
            for (int r = 0; r < height; r++) {
                expected.beginPut();
                expected.putTerm(0, INTEGERS[r]);
                expected.putTerm(1, URIS[r]);
                expected.commitPut();
            }
            return expected;
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
        for (int i = 0; i < 20; i++) {
            ResultJournal.clear();
            ThreadJournal.closeThreadJournals();
            try (var w = ThreadJournal.watchdog(System.out, 100)) {
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