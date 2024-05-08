package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItGenerator;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.bit.DedupConcatBIt;
import com.github.alexishuf.fastersparql.operators.bit.DedupMergeBIt;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Empty;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.Union;
import com.github.alexishuf.fastersparql.util.IntList;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsOrdered;
import static com.github.alexishuf.fastersparql.batch.IntsBatch.assertEqualsUnordered;
import static com.github.alexishuf.fastersparql.batch.type.TermBatchType.TERM;
import static java.util.List.of;
import static java.util.stream.Collectors.joining;

class WeakCrossSourceDedupBItTest {
    record D(List<int[]> sources, int[] expectedSequential, int minBatch, int maxBatch,
             BItGenerator gen, BItDrainer drainer, boolean sequential) implements Runnable {

        private BIt<TermBatch> createBIt() {
            List<BIt<TermBatch>> its = new ArrayList<>();
            Plan[] operands = new Plan[sources.size()];
            for (int i = 0; i < operands.length; i++) {
                its.add(gen.asBIt(it -> it.minBatch(minBatch).maxBatch(maxBatch), sources.get(i)));
                operands[i] = new Empty(Vars.of("x"), Vars.of("x"));
            }
            var union = new Union(false, operands);
            var dedup = Dedup.weakCrossSource(TERM, 1);
            Vars outVars = union.publicVars();
            Metrics m = Metrics.createIf(union);
            //noinspection resource
            return sequential ? new DedupConcatBIt<>(its, TERM, outVars, dedup).metrics(m)
                              : new DedupMergeBIt<>(its, TERM, outVars, m, dedup);
        }

        @Override public void run() {
            IntList ints = drainer.drainToInts(createBIt(), expectedSequential.length);
            if (sequential) {
                assertEqualsOrdered(expectedSequential, ints);
            } else {
                assertEqualsUnordered(expectedSequential, ints, true, false, false);
            }
            ints.clear();
        }

        @Override public String toString() {
            return "D{sources=[" +
                    sources.stream().map(Arrays::toString).collect(joining(", ")) +
                    "], expectedSequential=" + Arrays.toString(expectedSequential) +
                    ", minBatch=" + minBatch +
                    ", maxBatch=" + maxBatch +
                    ", gen=" + gen +
                    ", drainer=" + drainer +
                    ", sequential=" + sequential;
        }
    }

    static List<D> data() {
        record IO(List<int[]> sources, int[] expected) {}
        List<IO> sourcesLists = List.of(
                //single fully distinct source
                new IO(List.of(new int[]{1}), new int[]{1}),
                new IO(List.of(new int[]{1, 2}), new int[]{1, 2}),
                new IO(List.of(new int[]{}), new int[]{}),

                // two sources without duplicates
                new IO(List.of(new int[] {1   }, new int[] {2}),  new int[]{1, 2}),
                new IO(List.of(new int[] {    }, new int[] {2}),  new int[]{2}),
                new IO(List.of(new int[] {1, 2}, new int[] { }),  new int[]{1, 2}),

                // intra-source duplicates, distinct inter-sources
                new IO(List.of(new int[] {1, 1}, new int[] {2}),  new int[]{1, 1, 2}),
                new IO(List.of(new int[] {1   }, new int[] {2, 3, 2}),  new int[]{1, 2, 3, 2}),

                // cross-source duplicates
                new IO(List.of(new int[] {1, 2}, new int[]{1, 3}),  new int[]{1, 2, 3}),
                new IO(List.of(new int[] {1, 2}, new int[]{3, 2}),  new int[]{1, 2, 3}),
                new IO(List.of(new int[] {1, 2, 3}, new int[]{3, 4, 5, 1}), new int[]{1, 2, 3, 4, 5})
        );

        List<D> dList = new ArrayList<>();
        for (int minBatch : of(1, 2)) {
            for (int maxBatch : of(1<<14, 2)) {
                for (var generator : BItGenerator.GENERATORS) {
                    for (BItDrainer drainer : BItDrainer.ALL) {
                        for (boolean sequential : of(false, true)) {
                            for (var sources : sourcesLists)
                                dList.add(new D(sources.sources, sources.expected, minBatch, maxBatch, generator, drainer, sequential));
                        }
                    }
                }
            }
        }
        return dList;
    }

    @RepeatedTest(3)
    void test() throws Exception {
        try (var tasks = TestTaskSet.virtualTaskSet(getClass().getSimpleName())) {
            data().forEach(tasks::add);
        }
    }

    @Test
    void serialTest() {
        List<D> data = data();
        for (int i = 0; i < data.size(); i++) {
            try {
                data.get(i).run();
            } catch (AssertionFailedError e) {
                String msg = e.getMessage() + " at data[" + i + "]=" + data.get(i);
                Object ex = e.isExpectedDefined() ? e.getExpected().getEphemeralValue() : null;
                Object ac = e.  isActualDefined() ? e.  getActual().getEphemeralValue() : null;
                if (ex instanceof int[] a) ex = Arrays.toString(a);
                if (ac instanceof int[] a) ac = Arrays.toString(a);
                throw new AssertionFailedError(msg, ex, ac, e);
            }
        }
    }
}