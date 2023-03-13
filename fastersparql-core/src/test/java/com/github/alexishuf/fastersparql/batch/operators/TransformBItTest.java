package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.adapters.AbstractBItTest;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.adapters.ThrowingIterator;
import com.github.alexishuf.fastersparql.batch.base.SPSCBufferedBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.NotRowType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

class TransformBItTest extends AbstractBItTest {

    static abstract class TransformScenario extends Scenario {
        public TransformScenario(Scenario s) {
            super(s.size(), s.minBatch(), s.maxBatch(), s.drainer(), s.error());
        }
        public abstract BIt<Integer> it();
        @Override public String toString() {
            return super.toString().replace("BaseScenario", getClass().getSimpleName());
        }
    }
    static class IteratorTransformScenario extends TransformScenario {
        public IteratorTransformScenario(Scenario s) { super(s); }
        @Override public BIt<Integer> it() {
            var it = IntStream.range(0, size).boxed().iterator();
            return new IteratorBIt<>(ThrowingIterator.andThrow(it, error), NotRowType.INTEGER, Vars.EMPTY);
        }
    }
    static class CallbackTransformScenario extends TransformScenario {
        public CallbackTransformScenario(Scenario s) { super(s); }
        @Override public BIt<Integer> it() {
            var it = new SPSCBufferedBIt<>(NotRowType.INTEGER, Vars.EMPTY);
            Thread.ofVirtual().name("feed-"+this).start(() -> {
                for (int i = 0; i < size; i++)
                    it.feed(i);
                it.complete(error);
            });
            return it;
        }
    }

    @Override protected List<? extends Scenario> scenarios() {
        List<Function<Scenario, TransformScenario>> factories = List.of(
                IteratorTransformScenario::new, CallbackTransformScenario::new);
        List<TransformScenario> list = new ArrayList<>();
        for (Scenario base : baseScenarios()) {
            for (Function<Scenario, TransformScenario> factory : factories)
                list.add(factory.apply(base));
        }
        return list;
    }

    private static final class WithOffset extends FilteringTransformBIt<Integer, Integer> {
        private final int offset;
        public WithOffset(BIt<Integer> source, int offset) {
            super(source, NotRowType.INTEGER, source.vars());
            this.offset = offset;
        }

        @Override protected Batch<Integer> process(Batch<Integer> b) {
            Integer[] a = b.array;
            for (int i = 0, n = b.size; i < n; i++)
                a[i] = a[i] + offset;
            return b;
        }

        @Override protected String toStringNoArgs() { return "WithOffset["+offset+"]"; }
    }

    @Override protected void run(Scenario scenario) {
        var ts = (TransformScenario)scenario;
        var expected = IntStream.range(0, ts.size()).boxed().map(i -> 23 + i).toList();
        var transformed = new WithOffset(ts.it(), 23);
        ts.drainer().drainOrdered(transformed, expected, ts.error());
    }
}