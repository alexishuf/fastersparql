package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.CollectingReceiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.TwoSegmentRope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.BitSet;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class GatheringEmitterTest {
    private static final Vars X = Vars.of("x");
    private static final FinalSegmentRope PREFIX = SHARED_ROPES.internPrefix("<http://www.example.org/integers/");

    private static Orphan<CompressedBatch>
    makeExpected(int id, int height, int cancelAt, int failAt) {
        int rows = Math.min(height, Math.min(failAt, cancelAt));
        try (var g = new Guard.BatchGuard<CompressedBatch>(MAKE_EXPECTED);
             var local = PooledMutableRope.get()) {
            var expected = g.set(COMPRESSED.create(1));
            for (int i = 0; i < rows; i++) {
                expected.beginPut();
                local.clear().append((long)id*height + i).append('>');
                expected.putTerm(0, PREFIX, local.utf8, 0, local.len, false);
                expected.commitPut();
            }
            return g.take();
        }
    }
    private static final StaticMethodOwner MAKE_EXPECTED = new StaticMethodOwner("GatheringEmitterTest.makeExpected");

    private static class P extends TaskEmitter<CompressedBatch, P> implements Orphan<P> {
        private int absRow, relRow;
        private @Nullable CompressedBatch current;
        private final int cancelAt, failAt;

        public P(@NonNull CompressedBatch expected, int cancelAt, int failAt) {
            super(COMPRESSED, X, CREATED, TASK_FLAGS);
            assert expected.validate(Batch.Validation.CHEAP);
            this.current = expected;
            this.failAt = failAt;
            this.cancelAt = cancelAt;
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
        }

        @Override public P takeOwnership(Object newOwner) {
            return takeOwnership0(newOwner);
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            throw new UnsupportedOperationException();
        }

        @Override public Vars bindableVars() { return Vars.EMPTY; }

        @Override protected int produceAndDeliver(int state) {
            if (absRow == cancelAt)
                return CANCELLED;
            if (absRow == failAt)
                throw new RuntimeException("failAt");
            if (current != null && relRow >= current.rows) {
                relRow = 0;
                current = current.next;
                assert current == null || current.rows > 0;
            }
            if (current == null)
                return COMPLETED;
            deliver(current.dupRow(relRow));
            ++relRow;
            ++absRow;
            return state;
        }
    }

    record D(int producers, int height, int cancellingProducer, int cancelAt, int failingProducer, int failAt) implements Runnable {

        @Override public void run() {
            GatheringEmitter<CompressedBatch> gather = null;
            CollectingReceiver<CompressedBatch> receiver = null;
            CompressedBatch[] batches = new CompressedBatch[this.producers];
            CompressedBatch actual = null;
            Throwable error = null;
            try {
                gather = GatheringEmitter.create(COMPRESSED, X).takeOwnership(this);
                for (int i = 0; i < this.producers; i++) {
                    int cancelAt = i == cancellingProducer ? this.cancelAt : MAX_VALUE;
                    int   failAt = i ==    failingProducer ? this.failAt   : MAX_VALUE;
                    batches[i] = makeExpected(i, height, cancelAt, failAt).takeOwnership(this);
                    gather.subscribeTo(new P(batches[i], cancelAt, failAt));
                }
                receiver = CollectingReceiver.create(gather.releaseOwnership(this))
                                             .takeOwnership(this);
                gather = null;
                try {
                    receiver.join();
                } catch (CompletionException e) {
                    error = e.getCause();
                }
                actual = receiver.take().takeOwnership(this);
                assertTrue(actual.validate(Batch.Validation.CHEAP));

                // assert error matches expected
                if (failAt <= height) {
                    if (error == null)
                        fail("Expected an error");
                    if (!error.toString().contains("failAt"))
                        fail("Expected a simulated \"failAt\" exception, got", error);
                } else if (cancelAt <= height) {
                    if (error == null)
                        fail("Expected a cancellation exception, got nothing");
                    if (!(error instanceof FSCancelledException)
                            && !(error instanceof BatchQueue.CancelledException))
                        fail("Expected a cancellation exception, got", error);
                } else if (error != null) {
                    fail("Unexpected error");
                }

                // assert all rows come from a producer and there is no duplicates
                BitSet seen = new BitSet();
                TwoSegmentRope view = new TwoSegmentRope();
                for (var node = actual; node != null; node = node.next) {
                    for (int r = 0; r < node.rows; r++) {
                        if (node.getRopeView(r, 0, view)) {
                            if (!view.has(0, PREFIX, 0, PREFIX.len))
                                fail("Unexpected value at row " + r + ": " + node.toString(r));
                            long value = view.parseLong(PREFIX.len);
                            if (value < 0 || value > MAX_VALUE)
                                fail("Unexpected integer in " + view);
                            if (seen.get((int) value))
                                fail("Row " + r + " " + node.toString(r) + " is duplicate");
                            seen.set((int) value);
                        } else {
                            fail("Unset column 0 at row " + r);
                        }
                    }
                }

                // assert all produced rows were gathered
                for (int i = 0; i < producers; i++) {
                    int rows = Math.min(height,
                                        Math.min(cancellingProducer == i ? cancelAt : height,
                                                    failingProducer == i ?   failAt : height));
                    int base = i*height;
                    for (int r = 0; r < rows; r++) {
                        if (!seen.get(base + r))
                            fail("missing row "+r+" of producer "+i+" :"+batches[i].toString(r));
                    }
                }

            } finally {
                Owned.recycle(gather, this);
                Owned.recycle(receiver, this);
                Owned.recycle(actual, this);
                for (CompressedBatch b : batches)
                    Owned.recycle(b, this);
            }
        }
    }

    @BeforeAll static void beforeAll() { Batch.makeValidationCheaper(); }
    @AfterAll  static void  afterAll() { Batch.restoreValidationCheaper(); }

    static Stream<Arguments> test() {
        return Stream.of(
                new D(1, 1, 0, MAX_VALUE, 0, MAX_VALUE),
                new D(1, 8, 0, MAX_VALUE, 0, MAX_VALUE),
                new D(1, 256, 0, MAX_VALUE, 0, MAX_VALUE),

                new D(2, 1, 0, MAX_VALUE, 0, MAX_VALUE),
                new D(2, 8, 0, MAX_VALUE, 0, MAX_VALUE),
                new D(2, 256, 0, MAX_VALUE, 0, MAX_VALUE),

                new D(128, 1, 0, MAX_VALUE, 0, MAX_VALUE),
                new D(128, 8, 0, MAX_VALUE, 0, MAX_VALUE),
                new D(128, 256, 0, MAX_VALUE, 0, MAX_VALUE),

                new D(1, 4, 0, 2, 0, MAX_VALUE),
                new D(1, 4, 0, MAX_VALUE, 0, 2),
                new D(1, 4, 0, MAX_VALUE, 0, 4),

                new D(128, 4, 0, 2, 0, MAX_VALUE),
                new D(128, 4, 0, MAX_VALUE, 0, 2),
                new D(128, 4, 0, MAX_VALUE, 0, 4)
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(D d) {
        d.run();
        for (int i = 0; i < 4; i++) d.run();
    }

    @Test
    void testConcurrent() throws Exception {
        test(test().map(a -> (D)a.get()[0]).findFirst().orElseThrow());
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            test().map(a -> (D)a.get()[0]).forEach(d -> tasks.repeat(4, d));
        }
    }


}