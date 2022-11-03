package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.adapters.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.adapters.IteratorBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

class FlatMapBItTest {
    private abstract static class ItFac {
        abstract <T> BIt<T> create(List<T> list, Class<T> cls);
        @Override public String toString() { return getClass().getSimpleName(); }
    }
    private static final class IteratorFac extends ItFac {
        @Override public <T> BIt<T> create(List<T> list, Class<T> cls) {
            return new IteratorBIt<>(list, cls, Vars.EMPTY) {
                @Override public String toString() {
                    return "IteratorBIt(" + list + ")";
                }
            };
        }
    }
    private static final class CallbackFac extends ItFac {
        @Override public <T> BIt<T> create(List<T> list, Class<T> cls) {
            CallbackBIt<T> cb = new CallbackBIt<>(cls, Vars.EMPTY) {
                @Override public String toString() { return "CallbackBIt("+list+")"; }
            };
            Thread.ofVirtual().start(() -> {
                for (T value : list)
                    cb.feed(value);
                cb.complete(null);
            });
            return cb;
        }
    }


    record D(List<List<Integer>> in,  ItFac itFac,  BItDrainer drainer) {}

    static List<D> testData() {
        List<List<List<Integer>>> lists = List.of(
                List.of(),
                List.of(List.of(1)),
                List.of(List.of(1, 2)),
                List.of(List.of(1), List.of(2)),
                List.of(List.of(1), List.of(), List.of(3)),
                List.of(List.of(), List.of(1)),
                List.of(List.of(1), List.of()),
                List.of(List.of(1), List.of(), List.of())
        );
        List<D> data = new ArrayList<>();
        for (ItFac fac : List.of(new IteratorFac(), new CallbackFac())) {
            for (BItDrainer drainer : BItDrainer.all()) {
                for (List<List<Integer>> list : lists)
                    data.add(new D(list, fac, drainer));
            }
        }
        return data;
    }

    @Test
    void test() {
        for (D d : testData()) {
            List<Integer> expected = d.in.stream().flatMap(List::stream).toList();
            //noinspection unchecked
            var src = d.itFac.create(d.in, (Class<List<Integer>>) (Class<?>) List.class);
            var fm = new FlatMapBIt<>(Integer.class, src, Vars.EMPTY) {
                @Override protected BIt<Integer> map(List<Integer> input) {
                    return d.itFac.create(input, Integer.class);
                }
            };
            d.drainer.drainOrdered(fm, expected, null);
        }
    }

    static Stream<Arguments> testThrowFromMap() {
        return BItDrainer.all().stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testThrowFromMap(BItDrainer drainer) {
        var source = new IteratorBIt<>(List.of(1), Integer.class, Vars.EMPTY);
        RuntimeException ex = new RuntimeException("test");
        var fm = new FlatMapBIt<>(Integer.class, source, Vars.EMPTY) {
            @Override protected BIt<Integer> map(Integer input) {
                throw ex;
            }
        };
        drainer.drainOrdered(fm, List.of(), ex);
    }


}