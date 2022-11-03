package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;

import static java.util.stream.IntStream.range;

class FilteringTransformBItTest extends TransformBItTest {

    private static final class OddWithOffset extends FilteringTransformBIt<Integer, Integer> {
        public OddWithOffset(BIt<Integer> delegate) {
            super(delegate, Integer.class, delegate.vars());
        }
        @Override protected Batch<Integer> process(Batch<Integer> b) {
            int o = 0;
            Integer[] a = b.array;
            for (int i = 0, n = b.size; i < n; i++) {
                if (a[i] % 2 != 0)
                    a[o++] = a[i] + 23;
            }
            b.size = o;
            return b;
        }
    }

    @Override protected void run(Scenario scenario) {
        var ts = (TransformScenario) scenario;
        var expected = range(0, ts.size()).filter(i -> i % 2 > 0).map(i -> i + 23).boxed().toList();
        var transformed = new OddWithOffset(ts.it());
        ts.drainer().drainOrdered(transformed, expected, ts.error());
    }
}