package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatchType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.lang.Character.MAX_RADIX;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DedupTest {

    private static final String[] DOMAINS = {"example.org", "www.informatik.de", "somewhere.com"};
    private static final String[] PATHS = {"/#", "/ns#", "/d/", "/ontology/", "/graph/"};
    TermBatch generateRows(int n) {
        TermBatch batch = TermBatchType.TERM.create(1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.setLength(0);
            int type = i & 7;
            batch = switch (type) {
                case 0, 1, 2, 3 -> //IRI
                    batch.putRow(new Term[]{Term.valueOf(sb.append("<http://")
                                                       .append(DOMAINS[i % DOMAINS.length])
                                                       .append(PATHS[i % PATHS.length])
                                                       .append(Integer.toString(i, MAX_RADIX))
                                                       .append(">"))});
                case 4, 5, 6 -> { // plain literal
                    sb.append("\"");
                    for (int j = 0; j < type; j++)
                        sb.append(Long.toString((long)(Math.random()*Long.MAX_VALUE), MAX_RADIX));
                    sb.append(type == 4 ? "\"" : "\"@en");
                    yield batch.putRow(new Term[]{Term.valueOf(sb)});
                }
                case 7 -> { // typed literal
                    String lit = sb.append("\"").append(Math.random())
                                   .append("\"^^<http://www.w3.org/2001/XMLSchema#double>")
                                   .toString();
                    yield batch.putRow(new Term[]{Term.valueOf(lit)});
                }
                default -> batch;
            };
        }
        return batch;
    }

    static Stream<Arguments> testGenerateRows() {
        return test().map(a -> ((D)a.get()[0]).uniqueRows)
                .distinct().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testGenerateRows(int n) {
        TermBatch batch = generateRows(n);
        assertEquals(1, batch.cols);
        assertEquals(n, batch.totalRows());

        Map<Term, Integer> histogram = new HashMap<>();
        for (var node = batch; node != null; node = node.next) {
            for (int r = 0; r < node.rows; r++) {
                Term t = node.get(r, 0);
                histogram.put(t, 1 + histogram.getOrDefault(t, 0));
            }
        }
        var duplicates = histogram.entrySet().stream().filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey).toList();
        if (!duplicates.isEmpty())
            fail("duplicate rows: "+duplicates);
    }

    private static final Function<Integer, Dedup<TermBatch>> wFac = new Function<>() {
        @Override public Dedup<TermBatch> apply(Integer c) {
            return new WeakDedup<>(TermBatchType.TERM, 1);
        }
        @Override public String toString() { return "WeakDedup"; }
    };
    private static final Function<Integer, Dedup<TermBatch>> sFac = new Function<>() {
        @Override public Dedup<TermBatch> apply(Integer c) {
            return StrongDedup.strongForever(TermBatchType.TERM, c, 1);
        }
        @Override public String toString() { return "StrongDedup"; }
    };
    private static final Function<Integer, Dedup<TermBatch>> cFac = new Function<>() {
        @Override public Dedup<TermBatch> apply(Integer c) {
            return new WeakCrossSourceDedup<>(TermBatchType.TERM, 1);
        }
        @Override public String toString() { return "WeakCrossSourceDedup"; }
    };

    record D(int capacity, int threads, int uniqueRows, boolean twice, int repeatEvery) {  }

    static Stream<Arguments> test() {
        List<D> dList = new ArrayList<>();
        int processors = Runtime.getRuntime().availableProcessors();
        // single thread simple scenarios
        for (int rows : List.of(1, 8, 9)) {
            for (Integer capacity : List.of(8, 20)) {
                dList.add(new D(capacity, 1, rows, false, 0));
                dList.add(new D(capacity, 1, rows, true, 0));
                if (rows > 2)
                    dList.add(new D(capacity, 1, rows, true, 2));
            }
        }
        // multiple threads with many rows
        for (int threads : List.of(1, processors, 2 * processors)) {
            // test collisions and data races
            int rows = 1024*threads;
            dList.add(new D(8, threads, rows, true, 2));
            // excess capacity
            dList.add(new D(2*rows, threads, rows, true, 2));
        }

        List<Arguments> list = new ArrayList<>();

        for (D d : dList) list.add(arguments(d, wFac));
        for (D d : dList) list.add(arguments(d, cFac));
        for (D d : dList) list.add(arguments(d, sFac));
        return list.stream();
    }

    private void work(D d, TermBatch batch, int thread, Dedup<TermBatch> dedup) {
        int totalRows = batch.totalRows();
        int chunk = totalRows/d.threads;
        int begin = thread*chunk, end = thread == d.threads-1 ? totalRows : begin+chunk;

        TermBatch node = batch;
        int relRow = begin;
        for (; node != null && relRow >= node.rows; node = node.next) relRow -= node.rows;
        assertNotNull(node);

        int twiceFailures = 0;
        int repeatFailures = 0, repeatTries = 0;
        for (int absRow = begin; absRow < end; absRow++, relRow++) {
            if (relRow >= node.rows) {
                assertNotNull(node = node.next);
                relRow = 0;
            }
            if (thread == 0)
                assertFalse(dedup.contains(node, relRow));
            assertFalse(dedup.isDuplicate(node, relRow, thread));
            if (thread == 0 && !dedup.isWeak() && d.threads == 1)
                assertTrue(dedup.contains(node, relRow));
            if (d.twice && !dedup.isDuplicate(node, relRow, thread))
                twiceFailures++;
            if (d.repeatEvery > 0 && relRow%d.repeatEvery == 0
                                  && absRow-d.repeatEvery >= begin
                                  && relRow-d.repeatEvery >= 0) {
                repeatTries++;
                repeatFailures += !dedup.isDuplicate(node, relRow - d.repeatEvery, thread) ? 1 : 0;
            }
        }
        if (dedup.isWeak()) {
            if (d.threads == 1) {
                if (dedup instanceof WeakCrossSourceDedup<?>)
                    assertEquals(d.twice ? end-begin : 0, twiceFailures);
                else
                    assertEquals(0, twiceFailures);
            }
        } else {
            assertEquals(0, twiceFailures);
        }
        if (d.threads == 1) {
            if (dedup instanceof WeakCrossSourceDedup<?>) {
                assertEquals(repeatTries, repeatFailures);
            } else if (repeatTries > 100) {
                double failRatio = repeatFailures / (double) repeatTries;
                double maxFailRatio = 1 - (d.capacity / (4.0 * totalRows));
                assertTrue(failRatio < maxFailRatio);
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(D d, Function<Integer, Dedup<TermBatch>> factory) throws Exception {
        TermBatch rows = generateRows(d.uniqueRows);
        Dedup<TermBatch> add = factory.apply(d.capacity);
//        WeakDedup<TermBatch> dedup = new WeakDedup<>(ArrayRow.STRING, d.capacity);
//        RowHashWindowSet<String[]> dedup = new RowHashWindowSet<>(d.capacity, ArrayRow.STRING);
        if (d.threads > 1) {
            try (var e = Executors.newFixedThreadPool(d.threads)) {
                var tasks = IntStream.range(0, d.threads)
                        .mapToObj(i -> e.submit(() -> work(d, rows, i, add)))
                        .toList();
                for (Future<?> t : tasks)
                    t.get();
            }
        } else {
            work(d, rows, 0, add);
        }
    }


    static Stream<Arguments> testHasAndAdd() {
        return Stream.of(
                StrongDedup.strongUntil(TermBatchType.TERM, 128, 1),
                new WeakDedup<>(TermBatchType.TERM, 1),
                new WeakCrossSourceDedup<>(TermBatchType.TERM, 1)
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testHasAndAdd(Dedup<TermBatch> set) {
        var batch = TermBatch.rowMajor(termList("<a>", "_:bn", "23", "exns:bob", "\"alice\"@en"),
                                       5, 1);
        for (int r = 0; r < batch.rows; r++) {
            assertFalse(set.contains(batch, r));
            assertTrue(set.add(batch, r));
            assertTrue(set.contains(batch, r));
        }
        if (!set.isWeak()) {
            for (int r = 0; r < batch.rows; r++)
                assertTrue(set.contains(batch, r));
        }
    }

    public static void main(String[] args) throws Exception {
        double secs = 10;
        int threads = Runtime.getRuntime().availableProcessors();
        int capacity = 1024*threads;
        int rows     = 8*capacity;
        D d = new D(capacity, threads, rows, true, 2);
        DedupTest test = new DedupTest();
        System.out.println(d);

        long end = System.nanoTime()+(long) (secs*1_000_000_000L);
        int ops = 0;
        while (System.nanoTime() < end) {
            ++ops;
            test.test(d, sFac);
        }
        System.out.printf("%.3f ops/s\n", ops/secs);
    }

}