package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.plan.MergePlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import lombok.AllArgsConstructor;
import lombok.val;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.TestHelpers.checkRows;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class MergeTest {
    private static final RowOperations ROW_OPS = RowOperationsRegistry.get().forClass(Row.class);

    private static class Row extends ArrayList<String> {
        public Row(String... terms) { addAll(asList(terms)); }
    }
    private static Row r(int... values) {
        String[] strings = Arrays.stream(values).mapToObj(i -> i == 0 ? null : "<" + i + ">")
                                                .toArray(String[]::new);
        return new Row(strings);
    }

    @AllArgsConstructor
    private static class D {
        List<List<String>> inputsVars;
        List<List<Row>> inputs;
        long flags;
        List<Row> expected;

        public D(List<List<String>> inputsVars, List<List<Row>> inputs, List<Row> expected) {
            this(inputsVars, inputs, 0L, expected);
        }

        D withFlags(long flags) { return new D(inputsVars, inputs, flags, expected); }

        List<String> expectedVars() {
            LinkedHashSet<String> set = new LinkedHashSet<>();
            inputsVars.forEach(set::addAll);
            return new ArrayList<>(set);
        }
        List<Plan<Row>> inputPlans() {
            List<Plan<Row>> plans = new ArrayList<>();
            for (int i = 0; i < inputs.size(); i++) {
                int idx = i;
                plans.add(new Plan<Row>() {
                    @Override public String name() { return "input-"+idx; }
                    @Override public List<? extends Plan<Row>> operands() { return emptyList(); }
                    @Override public @Nullable Plan<Row> parent() { return null; }
                    @Override public Class<? super Row> rowClass() { return Row.class; }
                    @Override public List<String> publicVars() { return inputsVars.get(idx); }
                    @Override public List<String> allVars() { return inputsVars.get(idx); }
                    @Override public Results<Row> execute() {
                        val pub = FSPublisher.bindToAny(Flux.fromIterable(inputs.get(idx)));
                        return new Results<>(inputsVars.get(idx), Row.class, pub);
                    }
                    @Override public Plan<Row> bind(Binding binding) {
                        throw new UnsupportedOperationException();
                    }
                });
            }
            return plans;
        }

    }

    static Stream<Arguments> test() {
        List<D> base = asList(
                new D(singletonList(singletonList("x")), singletonList(singletonList(r(1))),
                      singletonList(r(1))),
                new D(singletonList(asList("x", "y")),
                      singletonList(singletonList(r(1, 2))),
                      singletonList(r(1, 2))),
                new D(asList(singletonList("x"), singletonList("x")),
                        asList(singletonList(r(1)),
                                singletonList(r(2))),
                        asList(r(1), r(2))),
                new D(asList(singletonList("x"), singletonList("x")),
                      asList(asList(r(1), r(2)),
                             asList(r(3), r(4))),
                      asList(r(1), r(2), r(3), r(4))),
                new D(asList(singletonList("x"), singletonList("y")),
                      asList(singletonList(r(1)), singletonList(r(2))),
                      asList(r(1, 0), r(0, 2))),
                new D(asList(singletonList("x"), asList("x", "y"), singletonList("y")),
                      asList(singletonList(r(11)),
                             asList(r(21, 22), r(23, 24)),
                             singletonList(r(31))),
                      asList(r(11, 0),
                             r(21, 22), r(23, 24),
                             r(0, 31)))
        );
        List<D> list = new ArrayList<>();
        for (long flags : asList(0L, OperatorFlags.ASYNC, OperatorFlags.SPILLOVER)) {
            for (D d : base)
                list.add(d.withFlags(flags));
        }
        return list.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void test(D d) {
        Merge op = FasterSparqlOps.create(Merge.class, d.flags, List.class);
        MergePlan<Row> plan = op.asPlan(d.inputPlans());
        checkRows(d.expected, d.expectedVars(), null, plan, false);
    }

}
