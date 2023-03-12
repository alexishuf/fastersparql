package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class WeakCrossSourceDedupTest {

    static Stream<Arguments> testHashtableSingleThread() {
        List<Arguments> list = new ArrayList<>();
        //why low capacities: more races, collisions and overflows
        for (Integer capacity : of(8, 9, 15)) {
            for (Integer rows : of(1, 8, 9, 1024))
                list.add(arguments(capacity, rows));
        }
        return list.stream();
    }

    private static Term[] prefixedRow(int n) {
        return new Term[] {Term.plainString("\"abcdef"+n+"\"")};
    }

    @ParameterizedTest @MethodSource
    void testHashtableSingleThread(int capacity, int rows) {
        var table = new WeakCrossSourceDedup<>(RowType.ARRAY, capacity);
        for (int row = 0; row < rows; row++) {
            assertFalse(table.isDuplicate(prefixedRow(row), 0));
            assertFalse(table.isDuplicate(prefixedRow(row), 32)); // share the same bit
            var array = prefixedRow(row);
            for (int source = 1; source < 32; source++)
                assertTrue(table.isDuplicate(array, source), "source="+source);
            assertTrue(table.isDuplicate(prefixedRow(row), 0));
        }
    }

    static Stream<Arguments> testHashtableConcurrent() {
        List<Arguments> list = new ArrayList<>();
        int processors = Runtime.getRuntime().availableProcessors();
        for (int multiplier : of(1, 2, 4))
            testHashtableSingleThread()
                    .map(a -> arguments(a.get()[0], a.get()[1], processors*multiplier))
                    .forEach(list::add);
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testHashtableConcurrent(int capacity, int rows, int threads) throws Exception {
        var table = new WeakCrossSourceDedup<>(RowType.ARRAY, capacity);
        List<Future<?>> tasks = new ArrayList<>();
        try (var executor = Executors.newFixedThreadPool(threads)) {
            IntStream.range(0, threads).forEach(thread -> tasks.add(executor.submit(() -> {
                for (int row = 0; row < rows; row++) {
                    for (int rep = 0; rep < threads; rep++) {
                        String ctx = "thread="+thread+", row="+row+ ", rep="+rep;
                        Term[] array = new Term[] {
                                Term.plainString("thread "+thread),
                                Term.plainString(" row"+row)
                        };
                        assertFalse(table.isDuplicate(array, thread), ctx);
                    }
                }
            })));
            for (Future<?> task : tasks)
                task.get();
        }
    }


}