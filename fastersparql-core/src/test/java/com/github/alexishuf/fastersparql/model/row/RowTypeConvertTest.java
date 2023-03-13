package com.github.alexishuf.fastersparql.model.row;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.AbstractBItTest;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.adapters.ThrowingIterator;
import com.github.alexishuf.fastersparql.batch.base.SPSCBufferedBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.ofVirtual;

class RowTypeConvertTest extends AbstractBItTest {
    private static List<Term[]> inputList(int size) {
        var list = new ArrayList<Term[]>(size);
        for (int i = 0; i < size; i++)
            list.add(new Term[]{Term.valueOf("<r"+i+"c0>"), Term.valueOf("<r"+i+"c1>")});
        return list;
    }

    private static List<List<Term>> expectedList(int size) {
        var list = new ArrayList<List<Term>>(size);
        for (int i = 0; i < size; i++)
            list.add(List.of(Term.valueOf("<r"+i+"c0>"), Term.valueOf("<r"+i+"c1>")));
        return list;
    }

    private static abstract class ConverterScenario extends Scenario {
        public ConverterScenario(Scenario base) {
            super(base.size(), base.minBatch(), base.maxBatch(), base.drainer(), base.error());
        }
        public abstract BIt<Term[]> it();
    }

    private abstract static class ItGenerator {
        public abstract BIt<Term[]> generate(int size, @Nullable Throwable error);
        @Override public String toString() { return getClass().getSimpleName(); }
    }

    private static final ItGenerator itGenerator = new ItGenerator() {
        @Override public BIt<Term[]> generate(int size, @Nullable Throwable error) {
            var it = ThrowingIterator.andThrow(inputList(size).iterator(), error);
            return new IteratorBIt<>(it, RowType.ARRAY, Vars.of("x", "y")) {
                @Override public String toString() {
                    return "IteratorBIt{size="+size+", error="+error+"}";
                }
            };
        }
    };

    private static final ItGenerator callbackGenerator = new ItGenerator() {
        @Override public BIt<Term[]> generate(int size, @Nullable Throwable error) {
            var it = new SPSCBufferedBIt<>(RowType.ARRAY, Vars.of("x", "y")) {
                @Override public String toString() {
                    return "SPSCBIt{size="+size+", error="+error+"}";
                }
            };
            ofVirtual().name("RowTypeConvertTest{size="+size+", error="+error+"}").start(() -> {
                inputList(size).forEach(it::feed);
                it.complete(error);
            });
            return it;
        }
    };


    private static final List<ItGenerator> generators = List.of(itGenerator, callbackGenerator);

    @Override protected List<? extends Scenario> scenarios() {
        List<ConverterScenario> list = new ArrayList<>();
        for (ItGenerator generator : generators) {
            for (Scenario base : baseScenarios()) {
                list.add(new ConverterScenario(base) {
                    @Override public BIt<Term[]> it() {
                        return generator.generate(size, error);
                    }
                });
            }
        }
        return list;
    }

    @Override protected void run(Scenario scenario) {
        var s = (ConverterScenario) scenario;
        var converted = RowType.LIST.convert(s.it());
        s.drainer().drainOrdered(converted, expectedList(s.size()), s.error());
    }
}