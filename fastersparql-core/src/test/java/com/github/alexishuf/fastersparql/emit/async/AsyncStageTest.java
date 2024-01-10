package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.DT_integer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class AsyncStageTest {
    private static final Vars X = Vars.of("x");

    private static final class P<B extends Batch<B>> extends TaskEmitter<B> {
        private final int begin, end;
        private int nextRow;
        private final @Nullable RuntimeException failCause;
        private final ByteRope nt = new ByteRope();

        public P(BatchType<B> batchType, int begin, int end, @Nullable RuntimeException failCause) {
            super(batchType, X, EMITTER_SVC, RR_WORKER, CREATED, TASK_FLAGS);
            this.begin = begin;
            this.nextRow = begin;
            this.end = end;
            this.failCause = failCause;
        }

        @Override public String label(StreamNodeDOT.Label type) {
            return STR."P(\{begin}:\{end}})@\{System.identityHashCode(this)}}";
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            throw new UnsupportedOperationException();
        }

        @Override public Vars bindableVars() {
            return Vars.EMPTY;
        }

        @Override protected int produceAndDeliver(int state) {
            B b = bt.createForThread(threadId, 1);
            for (int i = 0, n = 1+(nextRow&3); i < n && nextRow < end; i++) {
                nt.clear().append('"').append(nextRow++);
                b.beginPut();
                b.putTerm(0, DT_integer, nt, 0, nt.len, true);
                b.commitPut();
            }
            bt.recycle(deliver(b));
            if (nextRow == end) {
                nextRow++;
                if (failCause != null)
                    throw failCause;
                return COMPLETED;
            }
            return 0;
        }
    }

    private static final class C<B extends Batch<B>> implements Receiver<B> {
        private static final VarHandle RECEIVING;
        static {
            try {
                RECEIVING = MethodHandles.lookup().findVarHandle(C.class, "plainReceiving", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private final Emitter<B> upstream;
        private final BitSet received   = new BitSet();
        private final BitSet duplicates = new BitSet();
        private final boolean slow;
        private boolean cancelled;
        private @Nullable Throwable error = null;
        @SuppressWarnings("FieldMayBeFinal") private int plainReceiving = 0;
        private final SegmentRope view = new SegmentRope();
        private final Semaphore ready = new Semaphore(0);

        public C(Emitter<B> upstream, boolean slow) {
            this.slow     = slow;
            this.upstream = upstream;
            upstream.subscribe(this);
        }

        @Override public String label(StreamNodeDOT.Label type) {
            return STR."C@\{Integer.toHexString(System.identityHashCode(this))}";
        }

        public void check(int rows, @Nullable Throwable expectedFail) {
            upstream.request(Long.MAX_VALUE);
            ready.acquireUninterruptibly();

            assertEquals(rows,         received.cardinality());
            assertEquals(  -1,         received.nextSetBit(rows));
            assertEquals(   0,         duplicates.cardinality());
            assertEquals(expectedFail, error);
            assertFalse (cancelled);
        }

        @Override public @Nullable B onBatch(B batch) {
            long deadline = slow ? Timestamp.nextTick(2) : 0;
            if ((int)RECEIVING.compareAndExchangeAcquire(this, 0, 1) != 0)
                error = new AssertionError("concurrent onBatch");
            try {
                for (var b = batch; b != null; b = b.next) {
                    for (int r = 0, rows = b.rows; r < rows; r++) {
                        if (!b.localView(r, 0, view))
                            error = new AssertionError(STR."missing term at r=\{r}, c=0");
                        long num = view.parseLong(1);
                        if (num > Integer.MAX_VALUE || num < 0)
                            error = new AssertionError(STR."int overflow for num=\{num}");
                        int i = (int)num;
                        if (received.get(i)) duplicates.set(i);
                        else                 received  .set(i);
                    }
                }
            } catch (Throwable t) {
                error = t;
            } finally {
                if ((int)RECEIVING.compareAndExchangeRelease(this, 1, 0) != 1)
                    error = new AssertionError("concurrent onBatch");
            }
            if (slow) {
                while (Timestamp.nanoTime() <= deadline)
                    Thread.yield();
            }
            return batch;
        }

        @Override public void onComplete() {
            if ((int)RECEIVING.compareAndExchangeAcquire(this, 0, 1) != 0)
                error = new AssertionError("concurrent onComplete");
            ready.release();
            if ((int)RECEIVING.compareAndExchangeRelease(this, 1, 0) != 1)
                error = new AssertionError("concurrent onComplete");
        }

        @Override public void onCancelled() {
            if ((int)RECEIVING.compareAndExchangeAcquire(this, 0, 1) != 0)
                error = new AssertionError("concurrent onCancelled");
            cancelled = true;
            ready.release();
            if ((int)RECEIVING.compareAndExchangeRelease(this, 1, 0) != 1)
                error = new AssertionError("concurrent onCancelled");
        }

        @Override public void onError(Throwable cause) {
            if ((int)RECEIVING.compareAndExchangeAcquire(this, 0, 1) != 0)
                error = new AssertionError("concurrent onError");
            if (error == null) error = cause;
            ready.release();
            if ((int)RECEIVING.compareAndExchangeRelease(this, 1, 0) != 1)
                error = new AssertionError("concurrent onError");
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.of(upstream);
        }
    }

    public static Stream<Arguments> test() { return  data().stream(); }

    public static List<Arguments> data() {
        List<Arguments> list = new ArrayList<>();
        var dummyException = new RuntimeException("on purpose");
        int threads = Runtime.getRuntime().availableProcessors();
        record Dim(int rows, int producers) {}
        List<Dim> dims = new ArrayList<>();
        List.of(1, 2, 8, 64, 512, 2048).forEach(r -> dims.add(new Dim(r, 1)));
        List.of(2, threads, threads*2).forEach(p -> dims.add(new Dim(1, p)));
        dims.add(new Dim(2048, 2));
        for (var bt : List.of(TERM, COMPRESSED))
            dims.add(new Dim(bt.preferredTermsPerBatch()*2, threads));
        for (var bt : List.of(TERM, COMPRESSED)) {
            for (Dim dim : dims) {
                for (var fail : Arrays.asList(null, dummyException))
                    for (Boolean slow : List.of(false, true)) {
                        list.add(arguments(bt, dim.rows, dim.producers, slow, fail));
                    }
            }
        }
        return list;
    }

    @ParameterizedTest @MethodSource public <B extends Batch<B>>
    void test(BatchType<B> bt, int rows, int producers, boolean slow, RuntimeException fail) {
        doTest(bt, rows, producers, slow, fail);
        long minTimestamp = Timestamp.nanoTime()+100_000_000L;
        for (int i = 0; i < 10 || Timestamp.nanoTime() < minTimestamp; i++)
            doTest(bt, rows, producers, slow, fail);
    }

    @SuppressWarnings("unchecked")
    @Test <B extends Batch<B>> void testConcurrent() throws Exception {
        int repetitions = Runtime.getRuntime().availableProcessors()*4;
        List<Runnable> runnableList = new ArrayList<>();
        for (var args : data()) {
            BatchType<B> bt = (BatchType<B>)args.get()[0];
            int rows = (int)args.get()[1], producers = (int)args.get()[2];
            boolean slow = (boolean)args.get()[3];
            RuntimeException fail = (RuntimeException)args.get()[4];
            for (int rep = 0; rep < repetitions; rep++)
                runnableList.add(() -> doTest(bt, rows, producers, slow, fail));
        }
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            runnableList.forEach(tasks::add);
        }
    }


    private static <B extends Batch<B>> void doTest(BatchType<B> bt, int rows, int producers,
                                                    boolean slow, RuntimeException fail) {
        try (var w = ThreadJournal.watchdog(System.out, 100)) {
            ResultJournal.clear();
            ThreadJournal.resetJournals();
            w.start(5_000_000_000L);
            Emitter<B> root;
            if (producers == 1) {
                root = new AsyncStage<>(new P<>(bt, 0, rows, fail));
            } else {
                var ge = new GatheringEmitter<>(bt, X);
                root = ge;
                int begin = 0;
                for (int i = 0; i < producers; i++, begin+=rows)
                    ge.subscribeTo(new AsyncStage<>(new P<>(bt, begin, begin+rows, fail)));
            }
            new C<>(root, slow).check(rows*producers, fail);
        }
    }

}