package com.github.alexishuf.fastersparql.client.model.row;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.AbstractBItTest;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.batch.adapters.ThrowingIterator;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Thread.ofVirtual;

class RowTypeConvertTest extends AbstractBItTest {
    private record MyTerm(String value) {}
    private record MyRow(MyTerm[] terms) {
        @Override public boolean equals(Object obj) {
            return obj instanceof MyRow r && Arrays.equals(terms, r.terms);
        }
        @Override public int    hashCode() { return Arrays.hashCode(terms); }
        @Override public String toString() { return Arrays.toString(terms); }
    }

    private static class MyRowType extends RowType<MyRow, MyTerm> {
        public static final MyRowType INSTANCE = new MyRowType();
        public MyRowType() { super(MyRow.class, MyTerm.class); }

        @Override
        public @Nullable MyTerm set(MyRow row, int idx, @Nullable MyTerm value) {
            if (row == null)
                return null;
            MyTerm old = row.terms[idx];
            row.terms[idx] = value;
            return old;
        }

        @Override
        public @Nullable String setNT(MyRow row, int idx, @Nullable String nt) {
            if (row == null) return null;
            MyTerm old = row.terms[idx];
            row.terms[idx] = new MyTerm(nt);
            return old == null ? null : old.value;
        }

        @Override public @Nullable MyTerm get(@Nullable MyRow row, int idx) {
            return row == null ? null : row.terms[idx];
        }

        @Override public @Nullable String getNT(@Nullable MyRow row, int idx) {
            return row == null ? null : row.terms[idx].value;
        }

        @Override public MyRow createEmpty(Vars vars) {
            return new MyRow(new MyTerm[vars.size()]);
        }
    }

    private static final class MyTermArrayType extends ArrayRow<MyTerm> {
        public static final MyTermArrayType INSTANCE = new MyTermArrayType();
        public MyTermArrayType() { super(MyTerm.class); }

        @Override
        public @Nullable String setNT(MyTerm[] row, int idx, @Nullable String nt) {
            if (row == null) return null;
            MyTerm old = row[idx];
            return old.value;
        }

        @Override public @Nullable String getNT(MyTerm @Nullable [] row, int idx) {
            return row == null ? null : row[idx].value;
        }
    }


    private static List<MyTerm[]> inputList(int size) {
        var list = new ArrayList<MyTerm[]>(size);
        for (int i = 0; i < size; i++)
            list.add(new MyTerm[] {new MyTerm("<r"+i+"c0>"), new MyTerm("<r"+i+"c1>")});
        return list;
    }

    private static List<MyRow> expectedList(int size) {
        var list = new ArrayList<MyRow>(size);
        for (int i = 0; i < size; i++)
            list.add(new MyRow(new MyTerm[] { new MyTerm("<r"+i+"c0>"), new MyTerm("<r"+i+"c1>") }));
        return list;
    }

    private static abstract class ConverterScenario extends Scenario {
        public ConverterScenario(Scenario base) {
            super(base.size(), base.minBatch(), base.maxBatch(), base.drainer(), base.error());
        }
        public abstract BIt<MyTerm[]> it();
    }

    private abstract static class ItGenerator {
        public abstract BIt<MyTerm[]> generate(int size, @Nullable Throwable error);
        @Override public String toString() { return getClass().getSimpleName(); }
    }

    private static final ItGenerator itGenerator = new ItGenerator() {
        @Override public BIt<MyTerm[]> generate(int size, @Nullable Throwable error) {
            var it = ThrowingIterator.andThrow(inputList(size).iterator(), error);
            return new IteratorBIt<>(it, MyTerm[].class, Vars.of("x", "y")) {
                @Override public String toString() {
                    return "IteratorBIt{size="+size+", error="+error+"}";
                }
            };
        }
    };

    private static final ItGenerator callbackGenerator = new ItGenerator() {
        @Override public BIt<MyTerm[]> generate(int size, @Nullable Throwable error) {
            var it = new CallbackBIt<>(MyTerm[].class, Vars.of("x", "y")) {
                @Override public String toString() {
                    return "CallbackBIt{size="+size+", error="+error+"}";
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
                    @Override public BIt<MyTerm[]> it() {
                        return generator.generate(size, error);
                    }
                });
            }
        }
        return list;
    }

    @Override protected void run(Scenario scenario) {
        var s = (ConverterScenario) scenario;
        var converted = MyRowType.INSTANCE.convert(MyTermArrayType.INSTANCE, s.it());
        s.drainer().drainOrdered(converted, expectedList(s.size()), s.error());
    }
}