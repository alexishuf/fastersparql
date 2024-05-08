package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.UnsetError;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.concurrent.Watchdog;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Guard.BatchGuard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.SHARED_ROPES;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.jupiter.api.Assertions.*;

class CallbackEmitterTest {
    private static final Vars X = Vars.of("x");
    private static final FinalSegmentRope PREFIX = SHARED_ROPES.internPrefix("<http://www.example.org/integers/");

    private static abstract sealed class Cb extends CallbackEmitter<CompressedBatch, Cb> {
        private final Semaphore canFeed = new Semaphore(0);
        private final CompressedBatch expected;
        private final boolean selfCancel, fail;
        private boolean cancelled, released;
        private @MonotonicNonNull Future<?> feedTask;

        public static Orphan<Cb> create(Orphan<CompressedBatch> expected, boolean fail, boolean selfCancel) {
            return new Concrete(expected, fail, selfCancel);
        }
        protected Cb(Orphan<CompressedBatch> expected, boolean fail, boolean selfCancel) {
            super(COMPRESSED, X, EMITTER_SVC, RR_WORKER, CREATED, CB_FLAGS);
            this.expected = expected.takeOwnership(this);
            this.fail     = fail;
            this.selfCancel = selfCancel;
            if (ResultJournal.ENABLED)
                ResultJournal.initEmitter(this, vars);
        }

        private static final class Concrete extends Cb implements Orphan<Cb> {
            public Concrete(Orphan<CompressedBatch> expected, boolean fail, boolean selfCancel) {
                super(expected, fail, selfCancel);
            }
            @Override public Cb takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public @Nullable Cb recycle(Object currentOwner) {
            super.recycle(currentOwner);
            expected.recycle(this);
            return this;
        }

        public boolean isReleased() { return released; }

        private void feed() {
            boolean gotTerminated = false;
            for (var node = expected; node != null; node = node.next) {
                for (int r = 0; r < node.rows; r++) {
                    expected.requireOwner(this);
                    canFeed.acquireUninterruptibly();
                    canFeed.release();
                    if (cancelled)
                        break;
                    try {
                        offer(node.dupRow(r));
                    } catch (CancelledException e) {
                        break;
                    } catch (TerminatedException e) {
                        gotTerminated = true;
                    }
                }
            }
            journal("fed all rows, st=", state(), flags, "cb=", this);
            if (fail) {
                complete(new RuntimeException("test-fail"));
            } else if (cancelled) {
                cancel(true);
            } else if (selfCancel) {
                cancel();
                while (!cancelled)
                    canFeed.acquireUninterruptibly();
                cancel(true);
            } else {
                complete(null);
            }
            if (gotTerminated)
                throw new RuntimeException("got TerminatedException");
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            throw new UnsupportedOperationException();
        }

        @Override public Vars bindableVars() { return Vars.EMPTY; }

        @Override protected void startProducer() {
            journal("startProducer, st=", state(), flags, "cb=", this);
            super.onFirstRequest();
            feedTask = ForkJoinPool.commonPool().submit(this::feed);
        }

        @Override protected void resumeProducer(long requested) {
            journal("resumeProducer st=", state(), flags, "cb=", this);
            canFeed.release();
        }

        @Override protected void pauseProducer() {
            journal("pauseProducer st=", state(), flags, "cb=", this);
            canFeed.acquireUninterruptibly();
            canFeed.drainPermits();
        }

        @Override protected void cancelProducer() {
            journal("cancelProducer, st=", statePlain(), flags, "cb=", this);
            cancelled = true;
            canFeed.release();
        }

        @Override protected void earlyCancelProducer() {
            journal("earlyCancelProducer, st=", statePlain(), flags, "cb=", this);
        }

        @Override protected void releaseProducer() {
            journal("releaseProducer, st=", statePlain(), flags, "cb=", this);
            released = true;
        }
    }

    record D(int height, int cancelAt, int failAt) implements Runnable {
        @Override public void run() {
            Cb cb;
            String owner = Integer.toHexString((int)Double.doubleToLongBits(Math.random()));
            try (var local = PooledMutableRope.get();
                 var expectedGuard = new BatchGuard<CompressedBatch>(owner);
                 var actual = new BatchGuard<CompressedBatch>(owner);
                 var cbGuard = new Guard<Cb>(owner)) {
                actual.set(COMPRESSED.create(1));
                var expected = expectedGuard.set(COMPRESSED.create(1));
                for (int r = 0, n = Math.min(height, Math.min(failAt, cancelAt)); r < n; r++) {
                    expected.beginPut();
                    local.clear().append(r).append('>');
                    expected.putTerm(0, PREFIX, local.utf8, 0, local.len, false);
                    expected.commitPut();
                }
                cb = cbGuard.set(Cb.create(expected.dup(), failAt <= height,
                                             cancelAt <= height));
                Semaphore ready = new Semaphore(0);
                Throwable[] errorOrCancel = {null};
                AtomicInteger concurrentEvents = new AtomicInteger();
                AtomicBoolean terminated = new AtomicBoolean();
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
                    @Override public void onBatch(Orphan<CompressedBatch> orphan) {
                        int events = concurrentEvents.getAndIncrement();
                        try {
                            assertEquals(0, events);
                            assertFalse(terminated.get());
                            if (stats != null) stats.onBatchPassThrough(orphan);
                            cb.request(1);
                            actual.set(Batch.quickAppend(actual.get(), owner, orphan));
                        } finally {
                            concurrentEvents.compareAndExchangeRelease(1, 0);
                        }
                    }
                    @Override public void onBatchByCopy(CompressedBatch batch) {
                        int events = concurrentEvents.getAndIncrement();
                        try {
                            assertEquals(0, events);
                            assertFalse(terminated.get());
                            if (stats != null) stats.onBatchPassThrough(batch);
                            cb.request(1);
                            actual.get().copy(batch);
                        } finally {
                            concurrentEvents.compareAndExchangeRelease(1, 0);
                        }
                    }
                    @Override public void onComplete() {
                        int events = concurrentEvents.getAndIncrement();
                        boolean wasTerminated = terminated.compareAndExchange(false, true);
                        try {
                            assertEquals(0, events);
                            assertFalse(wasTerminated);
                        } finally {
                            concurrentEvents.compareAndExchangeRelease(1, 0);
                            ready.release();
                        }
                    }
                    @Override public void onCancelled() {
                        int events = concurrentEvents.getAndIncrement();
                        boolean wasTerminated = terminated.compareAndExchange(false, true);
                        try {
                            errorOrCancel[0] = new FSCancelledException();
                            assertEquals(0, events);
                            assertFalse(wasTerminated);
                        } finally {
                            concurrentEvents.compareAndExchangeRelease(1, 0);
                            ready.release();
                        }
                    }
                    @Override public void onError(Throwable cause) {
                        int events = concurrentEvents.getAndIncrement();
                        boolean wasTerminated = terminated.compareAndExchange(false, true);
                        try {
                            errorOrCancel[0] = cause == null ? new UnsetError() : cause;
                            assertEquals(0, events);
                            assertFalse(wasTerminated);
                        } finally {
                            concurrentEvents.compareAndExchangeRelease(1, 0);
                            ready.release();
                        }
                    }
                };
                cb.subscribe(receiver);
                cb.request(1);
                try (var w = Watchdog.spec("test").streamNode(receiver).create()) {
                    w.start(20_000_000_000L);
                    ready.acquireUninterruptibly();
                }
                assertEquals(0, concurrentEvents.get());
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
                assertEquals(expected, actual.get());
                assertDoesNotThrow(() -> cb.feedTask.get());
                assertFalse(cb.isReleased());
            }
            long deadline = Timestamp.nanoTime()+1_000_000_000L;
            while (!cb.isReleased() && deadline > Timestamp.nanoTime())
                Thread.yield();
            assertTrue(cb.isReleased(), "releaseProducer() not called");
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

    @RepeatedTest(10)
    void testConcurrent() throws Exception {
        test(test().map(a -> (D)a.get()[0]).findFirst().orElseThrow());
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            test().map(a -> (D)a.get()[0]).forEach(d -> tasks.repeat(32, d));
        }
    }


}