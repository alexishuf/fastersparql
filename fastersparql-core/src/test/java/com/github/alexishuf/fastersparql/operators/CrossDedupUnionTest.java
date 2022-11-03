package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.TestHelpers.checkRows;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CrossDedupUnionTest {

    private static class MyRowType extends RowType<MyRow, String> {
        public static final MyRowType INSTANCE = new MyRowType();
        public MyRowType() { super(MyRow.class, String.class); }
        @Override public @Nullable String set(MyRow row, int column, @Nullable String value) {
            return row == null ? null : row.set(column, value);
        }
        @Override public @Nullable String get(@Nullable MyRow row, int column) {
            return row == null ? null : row.get(column);
        }
        @Override public MyRow createEmpty(Vars vars) {
            return new MyRow(new String[vars.size()]);
        }
    }

    private static class MyRow extends ArrayList<String> {
        public MyRow(String... terms) { addAll(asList(terms)); }
    }
    private static MyRow r(int... values) {
        String[] strings = Arrays.stream(values).mapToObj(i -> i == 0 ? null : "<" + i + ">")
                                                .toArray(String[]::new);
        return new MyRow(strings);
    }

    record D(List<Vars> inputsVars, List<List<MyRow>> inputs, List<MyRow> expected) {
        List<String> expectedVars() {
            LinkedHashSet<String> set = new LinkedHashSet<>();
            inputsVars.forEach(set::addAll);
            return new ArrayList<>(set);
        }

        List<Plan<MyRow, String>> inputPlans() {
            return IntStream.range(0, inputs.size()).mapToObj(
                    i -> FSOps.values(MyRowType.INSTANCE, inputsVars.get(i), inputs.get(i))
            ).collect(Collectors.toList());
        }
    }

    static Stream<Arguments> test() {
        return Stream.of(
                new D(singletonList(Vars.of("x")), singletonList(singletonList(r(1))),
                      singletonList(r(1))),
                new D(singletonList(Vars.of("x", "y")),
                      singletonList(singletonList(r(1, 2))),
                      singletonList(r(1, 2))),
                new D(asList(Vars.of("x"), Vars.of("x")),
                        asList(singletonList(r(1)),
                                singletonList(r(2))),
                        asList(r(1), r(2))),
                new D(asList(Vars.of("x"), Vars.of("x")),
                      asList(asList(r(1), r(2)),
                             asList(r(3), r(4))),
                      asList(r(1), r(2), r(3), r(4))),
                new D(asList(Vars.of("x"), Vars.of("y")),
                      asList(singletonList(r(1)), singletonList(r(2))),
                      asList(r(1, 0), r(0, 2))),
                new D(asList(Vars.of("x"), Vars.of("x", "y"), Vars.of("y")),
                      asList(singletonList(r(11)),
                             asList(r(21, 22), r(23, 24)),
                             singletonList(r(31))),
                      asList(r(11, 0),
                             r(21, 22), r(23, 24),
                             r(0, 31)))
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(D d) {
        var plan = FSOps.crossDedupUnion(d.inputPlans());
        checkRows(d.expected, d.expectedVars(), null, plan, false);
    }

    @SuppressWarnings("resource") @Test
    void selfTest() {
        for (D d : test().map(a -> (D) a.get()[0]).toList()) {
            List<Plan<MyRow, String>> plans = d.inputPlans();
            for (int i = 0; i < plans.size(); i++)
                assertEquals(d.inputs.get(i), plans.get(i).execute().toList(), "i="+i);
        }
    }

}
