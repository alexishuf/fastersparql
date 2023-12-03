package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItGenerator;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.IntsBatch;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.ReceiverFuture;
import com.github.alexishuf.fastersparql.exceptions.RuntimeExecutionException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.BItGenerator.CB_GEN;
import static com.github.alexishuf.fastersparql.batch.BItGenerator.IT_GEN;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsOrdered;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsUnordered;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.jupiter.api.Assertions.fail;

public class BItProducerTest {
    private static final int THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final Vars X = Vars.of("x");

    private static final class DummyException extends RuntimeException { }

    private record D(Supplier<BIt<TermBatch>> itSupplier, int[] expected, boolean ordered,
                     Class<? extends Throwable> expectedErrCls) {
        public D(Supplier<BIt<TermBatch>> itSupplier, int[] expected) {
            this(itSupplier, expected, true, null);
        }

        public void testEmitter() {
            check(new IntsReceiver(new BItEmitter<>(itSupplier.get())));
        }

        private void check(IntsReceiver receiver) {
            try {
                receiver.getSimple();
                if (expectedErrCls != null)
                    fail("Expected "+expectedErrCls.getSimpleName());
            } catch (RuntimeExecutionException e) {
                Throwable inner = e.getCause();
                if (inner instanceof BItReadFailedException r)
                    inner = r.getCause();
                if (expectedErrCls == null)
                    fail("Unexpected error", inner);
                else if (expectedErrCls != inner.getClass())
                    fail("Expected "+ expectedErrCls.getSimpleName(), inner);
            }
            if (ordered) {
                assertEqualsOrdered(expected, receiver.ints, receiver.size);
            } else {
                assertEqualsUnordered(expected, receiver.ints, receiver.size,
                        false, false, false);
            }
        }

        private static final class IntsReceiver extends ReceiverFuture<int[], TermBatch> {
            private int[] ints = ArrayPool.intsAtLeast(16);
            private int size;

            public IntsReceiver(Emitter<TermBatch> emitter) { subscribeTo(emitter); }

            @Override public TermBatch onBatch(TermBatch batch) {
                Term view = Term.pooledMutable();
                for (var node = batch; node != null; node = node.next) {
                    for (int r = 0, rows = node.rows; r < rows; r++) {
                        if (!node.getView(r, 0, view))
                            continue;
                        if (size == ints.length)
                            ints = ArrayPool.grow(ints, size << 1);
                        ints[size++] = IntsBatch.parse(view);
                    }
                }
                view.recycle();
                return batch;
            }

            @Override public void onComplete() { complete(ints); }
        }
    }

    static Stream<Arguments> data() {
        List<D> list = new ArrayList<>();
        for (int[] ints : List.of(
                new int[0],
                new int[] {1},
                new int[] {1, 2},
                new int[] {1, 2, 3},
                new int[] {1, 2, 3, 4},
                new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
                )) {
            list.add(new D(() -> IT_GEN.asBIt(ints), ints));
            list.add(new D(() -> CB_GEN.asBIt(ints), ints));
            list.add(new D(() -> new MergeBIt<>(List.of(IT_GEN.asBIt(ints)), TERM, X), ints));
            list.add(new D(() -> new MergeBIt<>(List.of(CB_GEN.asBIt(ints)), TERM, X), ints));
        }
        list.add(new D(
                () -> new MergeBIt<>(
                        List.of(
                                IT_GEN.asBIt(1, 3, 5),
                                CB_GEN.asBIt(2, 4),
                                IT_GEN.asBIt(),
                                CB_GEN.asBIt(),
                                IT_GEN.asBIt(6)),
                        TERM, X),
                new int[] {1, 2, 3, 4, 5, 6}, false, null));
        for (BItGenerator gen : List.of(IT_GEN, CB_GEN)) {
            list.add(new D(() -> gen.asBIt(new DummyException(), 1, 2, 3),
                     new int[] {1, 2, 3}, true, DummyException.class));
        }

        return list.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("data")
    void testEmitter(D data) throws Exception {
        try (var ignored = ThreadJournal.watchdog(System.out, 50).start(2_000_000_000L)) {
            data.testEmitter();
        }
        TestTaskSet.virtualRepeatAndWait(getClass().getSimpleName(), THREADS, data::testEmitter);
    }

    @Test
    void testConcurrentEmitter() throws Exception {
        try (var tasks = new TestTaskSet(getClass().getSimpleName(), newFixedThreadPool(THREADS))) {
            data().map(a -> (D)a.get()[0]).forEach(d -> tasks.repeat(THREADS, d::testEmitter));
        }
    }
}
