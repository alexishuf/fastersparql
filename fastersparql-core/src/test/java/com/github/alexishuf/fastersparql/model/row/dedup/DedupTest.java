package com.github.alexishuf.fastersparql.model.row.dedup;

import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Character.MAX_RADIX;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DedupTest {

    private static final String[] DOMAINS = {"example.org", "www.informatik.de", "somewhere.com"};
    private static final String[] PATHS = {"/#", "/ns#", "/d/", "/ontology/", "/graph/"};
    List<Term[]> generateRows(int n) {
        List<Term[]> list = new ArrayList<>(n);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.setLength(0);
            int type = i & 7;
            switch (type) {
                case 0, 1, 2, 3 -> //IRI
                    list.add(new Term[]{Term.valueOf(sb.append("<http://")
                                                       .append(DOMAINS[i % DOMAINS.length])
                                                       .append(PATHS[i % PATHS.length])
                                                       .append(Integer.toString(i, MAX_RADIX))
                                                       .append(">"))});
                case 4, 5, 6 -> { // plain literal
                    sb.append("\"");
                    for (int j = 0; j < type; j++)
                        sb.append(Long.toString((long)(Math.random()*Long.MAX_VALUE), MAX_RADIX));
                    sb.append(type == 4 ? "\"" : "\"@en");
                    list.add(new Term[]{Term.valueOf(sb)});
                }
                case 7 -> { // typed literal
                    String lit = sb.append("\"").append(Math.random())
                                   .append("\"^^<http://www.w3.org/2001/XMLSchema#double>")
                                   .toString();
                    list.add(new Term[]{Term.valueOf(lit)});
                }
            }
        }
        return list;
    }

    static Stream<Arguments> testGenerateRows() {
        return test().map(a -> ((D)a.get()[0]).uniqueRows)
                .distinct().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testGenerateRows(int n) {
        List<Term[]> list = generateRows(n);
        Map<List<Term>, Integer> histogram = new HashMap<>();
        list.stream().map(Arrays::asList)
                .forEach(l -> histogram.put(l, 1+histogram.getOrDefault(l, 0)));
        var duplicates = histogram.entrySet().stream().filter(e -> e.getValue() > 1)
                .map(Map.Entry::getKey).toList();
        if (!duplicates.isEmpty())
            fail("duplicate rows: "+duplicates);
    }

    private static final Function<Integer, Dedup<Term[]>> wFac = new Function<>() {
        @Override public Dedup<Term[]> apply(Integer c) {
            return new WeakDedup<>(RowType.ARRAY, c);
        }
        @Override public String toString() { return "WeakDedup"; }
    };
    private static final Function<Integer, Dedup<Term[]>> sFac = new Function<>() {
        @Override public Dedup<Term[]> apply(Integer c) {
            return StrongDedup.strongForever(RowType.ARRAY, c);
        }
        @Override public String toString() { return "StrongDedup"; }
    };
    private static final Function<Integer, Dedup<Term[]>> cFac = new Function<>() {
        @Override public Dedup<Term[]> apply(Integer c) {
            return new WeakCrossSourceDedup<>(RowType.ARRAY, c);
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

        for (D d : dList) list.add(arguments(d, wFac, false));
        for (D d : dList) list.add(arguments(d, cFac, false));
        for (D d : dList) list.add(arguments(d, sFac, true));
        return list.stream();
    }

    private void work(D d, List<Term[]> rows, int thread, Dedup<Term[]> dedup,
                      boolean strict) {
        int chunk = rows.size()/d.threads;
        int begin = thread*chunk, end = thread == d.threads-1 ? rows.size() : begin+chunk;
        int twiceFailures = 0;
        int repeatFailures = 0, repeatTries = 0;
        for (int i = begin; i < end; i++) {
            if (thread == 0)
                assertFalse(dedup.contains(rows.get(i)));
            assertFalse(dedup.isDuplicate(rows.get(i), thread));
            if (thread == 0 && !dedup.isWeak() && d.threads == 1)
                assertTrue(dedup.contains(rows.get(i)));
            if (d.twice && !dedup.isDuplicate(rows.get(i), thread))
                twiceFailures++;
            if (d.repeatEvery > 0 && i % d.repeatEvery == 0 && i-d.repeatEvery >= begin) {
                repeatTries++;
                repeatFailures += !dedup.isDuplicate(rows.get(i - d.repeatEvery), thread) ? 1 : 0;
            }
        }
        if (strict) {
            assertEquals(0, twiceFailures);
            assertEquals(0, repeatFailures);
        } else if (d.threads == 1) {
            if (dedup instanceof WeakCrossSourceDedup<?>) {
                assertEquals(d.twice ? end-begin : 0, twiceFailures);
                assertEquals(repeatTries, repeatFailures);
            } else {
                assertEquals(0, twiceFailures);
                if (repeatTries > 100) {
                    double failRatio = repeatFailures / (double) repeatTries;
                    double maxFailRatio = 1 - (d.capacity / (4.0 * rows.size()));
                    assertTrue(failRatio < maxFailRatio);
                }
            }
        }
    }

    @ParameterizedTest @MethodSource
    void test(D d, Function<Integer, Dedup<Term[]>> factory, boolean strict) throws Exception {
        List<Term[]> rows = generateRows(d.uniqueRows);
        Dedup<Term[]> add = factory.apply(d.capacity);
//        WeakDedup<Term[]> dedup = new WeakDedup<>(ArrayRow.STRING, d.capacity);
//        RowHashWindowSet<String[]> dedup = new RowHashWindowSet<>(d.capacity, ArrayRow.STRING);
        if (d.threads > 1) {
            try (var e = Executors.newFixedThreadPool(d.threads)) {
                var tasks = IntStream.range(0, d.threads)
                        .mapToObj(i -> e.submit(() -> work(d, rows, i, add, strict)))
                        .toList();
                for (Future<?> t : tasks)
                    t.get();
            }
        } else {
            work(d, rows, 0, add, strict);
        }
    }


    static Stream<Arguments> testHasAndAdd() {
        return Stream.of(
                StrongDedup.strongUntil(RowType.ARRAY, 128),
                new WeakDedup<>(RowType.ARRAY, 128),
                new WeakCrossSourceDedup<>(RowType.ARRAY, 128)
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testHasAndAdd(Dedup<Term[]> set) {
        var terms = Term.termList("<a>", "_:bn", "23", "exns:bob", "\"alice\"@en");
        for (Term t : terms) {
            assertFalse(set.contains(new Term[]{t}));
            assertTrue(set.add(new Term[]{t}));
            assertTrue(set.contains(new Term[]{t}));
        }
        if (!set.isWeak()) {
            for (Term t : terms)
                assertTrue(set.contains(new Term[]{t}));
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
            test.test(d, sFac, true);
        }
        System.out.printf("%.3f ops/s\n", ops/secs);
    }

}