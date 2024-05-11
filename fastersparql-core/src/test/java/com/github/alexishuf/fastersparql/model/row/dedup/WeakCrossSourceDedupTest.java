package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Guard.BatchGuard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.RopeFactory.make;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class WeakCrossSourceDedupTest {

    static Stream<Arguments> testHashtableSingleThread() {
        return Stream.of(1, 8, 9, 1024).map(Arguments::arguments);
    }

    private static Orphan<TermBatch> prefixedRow(int n) {
        return TermBatch.of(termList("\"abcdef"+n+"\""));
    }

    @ParameterizedTest @ValueSource(ints = {1, 8, 9, 1024})
    void testHashtableSingleThread(int rows) {
        try (var tableGuard = new Guard<WeakCrossSourceDedup<TermBatch>>(this);
             var prefixedRowGuard = new BatchGuard<TermBatch>(this)) {
            var table = tableGuard.set(Dedup.weakCrossSource(TermBatchType.TERM, 1));
            for (int row = 0; row < rows; row++) {
                var batch = prefixedRowGuard.set(prefixedRow(row));
                assertFalse(table.isDuplicate(batch, 0, 0));
                assertFalse(table.isDuplicate(batch, 0, 32)); // share the same bit
                for (int source = 1; source < 32; source++)
                    assertTrue(table.isDuplicate(batch, 0, source));
                assertTrue(table.isDuplicate(batch, 0, 0));
            }
        }
    }

    static Stream<Arguments> testHashtableConcurrent() {
        List<Arguments> list = new ArrayList<>();
        int processors = Runtime.getRuntime().availableProcessors();
        for (int multiplier : of(1, 2, 4))
            testHashtableSingleThread()
                    .map(a -> arguments(a.get()[0], processors*multiplier))
                    .forEach(list::add);
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testHashtableConcurrent(int rows, int threads) throws Exception {
        List<Future<?>> tasks = new ArrayList<>();
        try (var tableGuard = new Guard<WeakCrossSourceDedup<TermBatch>>(this);
             var executor = Executors.newFixedThreadPool(threads)) {
            var table = tableGuard.set(Dedup.weakCrossSource(TermBatchType.TERM, 2));
            IntStream.range(0, threads).forEach(thread -> tasks.add(executor.submit(() -> {
                for (int row = 0; row < rows; row++) {
                    var threadNT = make(8+6).add("\"thread").add(thread).add('"').take();
                    var rowNT = make(5+6).add("\"row").add(thread).add('"').take();
                    var threadTerm = Term.wrap(threadNT, EMPTY);
                    var rowTerm = Term.wrap(rowNT, EMPTY);
                    for (int rep = 0; rep < threads; rep++) {
                        var batch = TermBatchType.TERM.create(2).takeOwnership(this);
                        batch.beginPut();
                        if (rep == 0) {
                            batch.putTerm(0, Term.wrap(threadNT, EMPTY));
                            batch.putTerm(1, Term.wrap(rowNT, EMPTY));
                        } else {
                            batch.putTerm(0, threadTerm);
                            batch.putTerm(1, rowTerm);
                        }
                        batch.commitPut();
                        assertFalse(table.isDuplicate(batch, 0, thread));
                        batch.recycle(this);
                    }
                }
            })));
            for (Future<?> task : tasks)
                task.get();
        }
    }
}