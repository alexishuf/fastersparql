package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.operators.bit.DedupConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.DedupMergeBIt;
import com.github.alexishuf.fastersparql.operators.plan.Empty;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Union;
import org.junit.jupiter.api.RepeatedTest;

import java.util.*;

import static java.lang.String.format;
import static java.util.List.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WeakCrossSourceDedupBItTest {

    static List<String> r(String value) {return List.of(value);}

    interface ItGenerator {
        BIt<List<String>> create(List<List<String>> sources);
    }

    private static final ItGenerator itGenerator = new ItGenerator() {
        @Override public BIt<List<String>> create(List<List<String>> list) {
            return new IteratorBIt<>(list, List.class, Vars.of("x")) {
                @Override public String toString() { return "IteratorBIt(" + list + ")"; }
            };
        }
        @Override public String toString() { return "itGenerator"; }
    };

    private static final ItGenerator cbGenerator = new ItGenerator() {
        @Override public BIt<List<String>> create(List<List<String>> list) {
            //noinspection unchecked
            var cls = (Class<List<String>>) (Class<?>) List.class;
            CallbackBIt<List<String>> it = new CallbackBIt<>(cls, Vars.of("x")) {
                @Override public String toString() { return "Callback("+list+")"; }
            };
            Thread.startVirtualThread(() -> {
                for (List<String> r : list)
                    it.feed(r);
                it.complete(null);
            });
            return it;
        }
        @Override public String toString() { return "cbGenerator"; }
    };


    private static final Map<List<List<List<String>>>, Map<String, Integer>> minCountCache
            = Collections.synchronizedMap(new IdentityHashMap<>());

    private static  Map<String, Integer> minCount(List<List<List<String>>> inputs) {
        Map<String, Integer> map = minCountCache.get(inputs);
        if (map == null) {
            map = new HashMap<>();
            Map<String, Integer> tmp = new HashMap<>();
            for (List<List<String>> source : inputs) {
                tmp.clear();
                for (List<String> row : source)
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

    record D(List<List<List<String>>> inputs, int minBatch, int maxBatch,
             ItGenerator generator, BItDrainer drainer, boolean sequential) implements Runnable {

        private BIt<List<String>> createBIt() {
            List<BIt<List<String>>> its = new ArrayList<>();
            List<Plan<List<String>, String>> operands = new ArrayList<>();
            for (List<List<String>> input : inputs) {
                its.add(generator.create(input));
                operands.add(new Empty<>(ListRow.STRING, Vars.of("x"), Vars.of("x"), null, null));
            }
            var union = new Union<>(operands, 0, null, null);
            var dedup = new WeakCrossSourceDedup<>(ListRow.STRING, 3);
            if (sequential)
                return new DedupConcatBIt<>(its, union, dedup);
            else
                return new DedupMergeBIt<>(its, union, dedup);
        }

        @Override public void run() {
            // compute expected
            Map<String, Integer> minCount = minCount(inputs);

            Map<String, Integer> actual = new HashMap<>();
            for (List<String> row : drainer.toList(createBIt()))
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
        List<List<List<List<String>>>> sourcesList = new ArrayList<>(of(
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
        List<List<List<String>>> longSources = new ArrayList<>();
        for (int source = 0; source < 20; source++) {
            List<List<String>> rows = new ArrayList<>();
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