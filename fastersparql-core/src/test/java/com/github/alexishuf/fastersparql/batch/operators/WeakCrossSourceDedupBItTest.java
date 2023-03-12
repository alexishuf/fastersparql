package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.model.row.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.operators.bit.DedupConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.DedupMergeBIt;
import com.github.alexishuf.fastersparql.operators.plan.Empty;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Union;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import org.junit.jupiter.api.RepeatedTest;

import java.util.*;

import static java.lang.String.format;
import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WeakCrossSourceDedupBItTest {

    static List<Term> r(CharSequence... value) {
        TermParser parser = new TermParser();
        return Arrays.stream(value).map(s -> parser.parseTerm(Rope.of(s))).toList();
    }

    interface ItGenerator {
        BIt<List<Term>> create(List<List<Term>> sources);
    }

    private static final ItGenerator itGenerator = new ItGenerator() {
        @Override public BIt<List<Term>> create(List<List<Term>> list) {
            return new IteratorBIt<>(list, RowType.LIST, Vars.of("x")) {
                @Override public String toString() { return "IteratorBIt(" + list + ")"; }
            };
        }
        @Override public String toString() { return "itGenerator"; }
    };

    private static final ItGenerator cbGenerator = new ItGenerator() {
        @Override public BIt<List<Term>> create(List<List<Term>> list) {
            CallbackBIt<List<Term>> it = new CallbackBIt<>(RowType.LIST, Vars.of("x")) {
                @Override public String toString() { return "Callback("+list+")"; }
            };
            Thread.startVirtualThread(() -> {
                for (List<Term> r : list)
                    it.feed(r);
                it.complete(null);
            });
            return it;
        }
        @Override public String toString() { return "cbGenerator"; }
    };


    private static final Map<List<List<List<Term>>>, Map<Term, Integer>> minCountCache
            = Collections.synchronizedMap(new IdentityHashMap<>());

    private static  Map<Term, Integer> minCount(List<List<List<Term>>> inputs) {
        Map<Term, Integer> map = minCountCache.get(inputs);
        if (map == null) {
            map = new HashMap<>();
            Map<Term, Integer> tmp = new HashMap<>();
            for (List<List<Term>> source : inputs) {
                tmp.clear();
                for (List<Term> row : source)
                    tmp.put(row.get(0), tmp.getOrDefault(row.get(0), 0)+1);
                for (var e : tmp.entrySet()) {
                    if (map.getOrDefault(e.getKey(), Integer.MAX_VALUE) > e.getValue())
                        map.put(e.getKey(), e.getValue());
                }
            }
            minCountCache.put(inputs, map);
        }
        return map;
    }

    record D(List<List<List<Term>>> inputs, int minBatch, int maxBatch,
             ItGenerator generator, BItDrainer drainer, boolean sequential) implements Runnable {

        private BIt<List<Term>> createBIt() {
            List<BIt<List<Term>>> its = new ArrayList<>();
            Plan[] operands = new Plan[inputs.size()];
            for (int i = 0; i < operands.length; i++) {
                its.add(generator.create(inputs.get(i)));
                operands[i] = new Empty(Vars.of("x"), Vars.of("x"));
            }
            var union = new Union(0, operands);
            var dedup = new WeakCrossSourceDedup<>(RowType.LIST, 3);
            if (sequential)
                return new DedupConcatBIt<>(its, union, dedup);
            else
                return new DedupMergeBIt<>(its, union, dedup);
        }

        @Override public void run() {
            // compute expected
            Map<Term, Integer> minCount = minCount(inputs);

            Map<Term, Integer> actual = new HashMap<>();
            for (List<Term> row : drainer.toList(createBIt()))
                actual.put(row.get(0), actual.getOrDefault(row.get(0), 0)+1);

            assertEquals(minCount.keySet(), actual.keySet());
            for (var e : minCount.entrySet()) {
                int count = actual.getOrDefault(e.getKey(), 0);
                int min = e.getValue();
                assertTrue(count >= min, format("%d instances of %s, expected at least %d", count, e.getKey(), min));
            }
        }
    }

    static List<D> data() {
        List<List<List<List<Term>>>> sourcesList = new ArrayList<>(of(
                //fully distinct cases
                of(of(r("1"))), // single, non-empty source
                of(of()),           // single, empty source
                of(of(r("1"), r("2"))),           // single 2-source
                of(of(r("1")), of(r("2"))),  // 2 1-source

                // cross-source duplicates
                of(of(r("1")), of(r("2"), r("1")), of(r("3"), r("1"))),
                of(of(r("1"), r("1")),
                   of(r("2"), r("1")),
                   of(r("3"), r("1")),
                   of(r("2")))
        ));
        List<List<List<Term>>> longSources = new ArrayList<>();
        for (int source = 0; source < 20; source++) {
            List<List<Term>> rows = new ArrayList<>();
            for (int row = 0; row < 256; row++) {
                rows.add(r(""+row));
                rows.add(r(""+row));
            }
            for (int row = 0; row < 128; row++)
                rows.add(r(""+row));
            longSources.add(rows);
        }
        sourcesList.add(longSources);

        List<D> dList = new ArrayList<>();
        for (int minBatch : of(1, 2)) {
            for (int maxBatch : of(32_768, 2)) {
                for (var generator : of(itGenerator, cbGenerator)) {
                    for (BItDrainer drainer : BItDrainer.all()) {
                        for (boolean sequential : of(false, true)) {
                            for (var sources : sourcesList)
                                dList.add(new D(sources, minBatch, maxBatch, generator, drainer, sequential));
                        }
                    }
                }
            }
        }
        return dList;
    }

    @RepeatedTest(3)
    void test() throws Exception {
        try (var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            data().forEach(tasks::add);
        }
    }
}