package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItGenerator;
import com.github.alexishuf.fastersparql.batch.adapters.AbstractBItTest;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.batch.IntsBatch.ints;

public abstract class AbstractMergeBItTest extends AbstractBItTest {
    protected static final class MergeScenario extends Scenario {
        private final List<BIt<TermBatch>> operands;
        private final int[] expectedInts;

        public MergeScenario(Scenario other, List<BIt<TermBatch>> operands,
                             int[] expected) {
            super(other);
            this.operands = operands;
            this.expectedInts = expected;
        }

        public List<BIt<TermBatch>> operands() { return operands; }
        @Override public int[] expectedInts() { return expectedInts; }

        @Override public String toString() {
            return "MergeScenario{size="+size+", minBatch="+minBatch
                    +", maxBatch="+maxBatch+", drainer="+drainer+", error="+error
                    +", operands="+operands+", expected="+Arrays.toString(expectedInts)+'}';
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MergeScenario that)) return false;
            if (!super.equals(o)) return false;
            return operands.equals(that.operands) && Arrays.equals(expectedInts, that.expectedInts);
        }

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), operands, Arrays.hashCode(expectedInts));
        }
    }

    private static final int[] INTS_012356 = {0, 1, 2, 3, 5, 6};

    @Override protected List<? extends Scenario> scenarios() {
        List<MergeScenario> scenarios = new ArrayList<>();

        // no-operand
        scenarios.add(new MergeScenario(new Scenario(0, 1, 1,
                                                     BItDrainer.RECYCLING, null),
                                        List.of(), new int[0]));
        //single operand
        for (var generator : BItGenerator.GENERATORS) {
            for (Scenario base : baseScenarios()) {
                var operands = List.of(generator.asBIt(base.error(), ints(base.size())));
                scenarios.add(new MergeScenario(base, operands, base.expectedInts()));
            }
        }

        // long chain of operators using all generators
        for (Integer step : List.of(0, 1, 2, 3, 7)) {
            for (var bs : batchSizes()) {
                for (BItDrainer drainer : BItDrainer.ALL) {
                    for (var error : Arrays.asList(null, new RuntimeException("test"))) {
                        int nOperands = 3 * BItGenerator.GENERATORS.size();
                        List<BIt<TermBatch>> operands = new ArrayList<>();
                        int[] expected = ints(nOperands * step);
                        for (int i = 0, start = 0; i < nOperands; i++, start += step) {
                            var opError = i == nOperands - 1 ? error : null;
                            var gen = BItGenerator.GENERATORS.get(i % BItGenerator.GENERATORS.size());
                            operands.add(gen.asBIt(opError, ints(start, step)));
                        }
                        var base = new Scenario(step, bs.min(), bs.max(), drainer, error);
                        scenarios.add(new MergeScenario(base, operands, expected));
                    }
                }
            }
        }

        //test change in sizes of operators
        for (BItDrainer drainer : BItDrainer.ALL) {
            for (var bs : batchSizes()) {
                List<BIt<TermBatch>> ops = new ArrayList<>();
                ops.add(BItGenerator.CB_GEN.asBIt(ints(0, 4))); // [0,1,2,3]
                ops.add(BItGenerator.IT_GEN.asBIt(ints(5, 2))); // [5,6]
                var base = new Scenario(4, bs.min(), bs.max(), drainer, null);
                scenarios.add(new MergeScenario(base, ops, INTS_012356));
            }
        }

        return scenarios;
    }
}
