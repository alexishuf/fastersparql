package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.UnsetError;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.jupiter.api.Assertions.*;

class CallbackEmitterTest {
    private static final Vars X = Vars.of("x");
    private static final SegmentRope PREFIX = SHARED_ROPES.internPrefix("<http://www.example.org/integers/");

    private static final class Cb extends CallbackEmitter<CompressedBatch> {
        private final Semaphore canFeed = new Semaphore(0);
        private final CompressedBatch expected;
        private final boolean cancel, fail;
        private @MonotonicNonNull Future<?> feedTask;

        public Cb(CompressedBatch expected, boolean fail, boolean cancel) {
            super(COMPRESSED, X, EMITTER_SVC, RR_WORKER, CREATED, TASK_FLAGS);
            this.expected = expected;
            this.fail     = fail;
            this.cancel   = cancel;
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
        }

        private void feed() {
            boolean gotTerminated = false;
            for (var node = expected; node != null; node = node.next) {
                for (int r = 0; r < node.rows; r++) {
                    canFeed.acquireUninterruptibly();
                    canFeed.release();
                    try {
                        if ((r & 1) == 0) {
                            COMPRESSED.recycle(offer(node.dupRow(r)));
                        } else {
                            putRow(node, r);
                        }
                    } catch (CancelledException e) {
                        break;
                    } catch (TerminatedException e) {
                        gotTerminated = true;
                    }
                }
            }
            journal("fed all rows, st=", state(), flags, "cb=", this);
            if      (fail)   complete(new RuntimeException("test-fail"));
            else if (cancel) cancel();
            else             complete(null);
            if (gotTerminated)
                throw new RuntimeException("got TerminatedException");
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            throw new UnsupportedOperationException();
        }

        @Override public Vars bindableVars() { return Vars.EMPTY; }

        @Override protected void onFirstRequest() {
            journal("onFirstRequest, st=", state(), flags, "cb=", this);
            super.onFirstRequest();
            feedTask = ForkJoinPool.commonPool().submit(this::feed);
        }

        @Override protected void pause() {
            journal("pause() st=", state(), flags, "cb=", this);
            canFeed.acquireUninterruptibly();
            canFeed.drainPermits();
        }

        @Override protected void resume() {
            journal("resume() st=", state(), flags, "cb=", this);
            canFeed.release();
        }
    }

    record D(int height, int cancelAt, int failAt) implements Runnable {
        @Override public void run() {
            ByteRope local = new ByteRope();
            var expected = COMPRESSED.create(1);
            for (int r = 0, n = Math.min(height, Math.min(failAt, cancelAt)); r < n; r++) {
                expected.beginPut();
                local.clear().append(r).append('>');
                expected.putTerm(0, PREFIX, local.utf8, 0, local.len, false);
                expected.commitPut();
            }
            var copy = expected.dup();
            CompressedBatch[] actual = {COMPRESSED.create(1)};
            actual[0].reserveAddLocals(expected.localBytesUsed());
            try {
                Cb cb = new Cb(copy, failAt <= height, cancelAt <= height);
                Semaphore ready = new Semaphore(0);
                Throwable[] errorOrCancel = {null};
                var receiver = new Receiver<CompressedBatch>() {
                    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

                    @Override public String label(StreamNodeDOT.Label type) {
                        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
                        if (stats != null && type.showStats())
                            stats.appendToLabel(sb);
                        return sb.toString();
                    }

                    @Override public Stream<? extends StreamNode> upstreamNodes() {
                        return Stream.of(cb);
                    }
                    @Override public CompressedBatch onBatch(CompressedBatch batch) {
                        if (stats != null) stats.onBatchPassThrough(batch);
                        cb.request(1);
                        actual[0].copy(batch);
                        return batch;
                    }
                    @Override public void onComplete() {
                        ready.release();
                    }
                    @Override public void onCancelled() {
                        errorOrCancel[0] = new FSCancelledException();
                        ready.release();
                    }
                    @Override public void onError(Throwable cause) {
                        errorOrCancel[0] = cause == null ? new UnsetError() : cause;
                        ready.release();
                    }
                };
                cb.subscribe(receiver);
                cb.request(1);
                try (var w = ThreadJournal.watchdog(System.out, 100)) {
                    w.start(20_000_000_000L);
                    ready.acquireUninterruptibly();
                }
                if (failAt <= height) {
                    if (errorOrCancel[0] == null)
                        fail("Expected onError()");
                    else if (!errorOrCancel[0].getMessage().contains("test-fail"))
                        fail("Actual error is not the expected", errorOrCancel[0]);
                } else if (cancelAt <= height) {
                    if (errorOrCancel[0] == null || !(errorOrCancel[0] instanceof FSCancelledException))
                        fail("Expected FSCancelledException, got", errorOrCancel[0]);
                } else if (errorOrCancel[0] != null) {
                    fail("Unexpected error/cancel", errorOrCancel[0]);
                }
                assertEquals(expected, actual[0]);
                assertDoesNotThrow(() -> cb.feedTask.get());
            } finally {
                COMPRESSED.recycle(expected);
                COMPRESSED.recycle(copy);
                COMPRESSED.recycle(actual[0]);
            }
        }
    }

    static Stream<Arguments> test() {
        return Stream.of(
                new D(1, MAX_VALUE, MAX_VALUE),
                new D(2, MAX_VALUE, MAX_VALUE),
                new D(8, MAX_VALUE, MAX_VALUE),
                new D(256, MAX_VALUE, MAX_VALUE),
                new D(256, 1, MAX_VALUE),
                new D(256, 8, MAX_VALUE),
                new D(256, 128, MAX_VALUE),
                new D(256, 200, MAX_VALUE),
                new D(256, MAX_VALUE, 1),
                new D(256, MAX_VALUE, 8),
                new D(256, MAX_VALUE, 128),
                new D(256, MAX_VALUE, 200)
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(D d) {
        ThreadJournal.resetJournals();
        ResultJournal.clear();
        d.run();
        for (int i = 0; i < 32; i++) {
            ThreadJournal.resetJournals();
            ResultJournal.clear();
            d.run();
        }
    }

    @Test
    void testConcurrent() throws Exception {
        test(test().map(a -> (D)a.get()[0]).findFirst().orElseThrow());
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            test().map(a -> (D)a.get()[0]).forEach(d -> tasks.repeat(32, d));
        }
    }


}