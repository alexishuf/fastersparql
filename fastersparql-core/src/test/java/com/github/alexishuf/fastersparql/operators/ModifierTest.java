package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.util.VThreadTaskSet;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprParser;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.operators.TestHelpers.*;
import static java.lang.Long.MAX_VALUE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

public class ModifierTest {
    private static final int N_ITERATIONS = 3;
    private static final int N_THREADS = Runtime.getRuntime().availableProcessors();

    private static Modifier<List<String>, String> modifier(List<List<String>> in, Vars projection,
                                                    int distinctCapacity, long offset, long limit,
                                                    String... filters) {
        assertNotEquals(0, limit, "nonsense");
        ExprParser p = new ExprParser();
        List<Expr> expressions = new ArrayList<>(filters.length);
        for (String string : filters)
            expressions.add(p.parse(string));
        var plan = asPlan(in.isEmpty() ? generateVars(3) : generateVars(in), in);
        return new Modifier<>(plan, projection, distinctCapacity,
                              offset, limit, expressions, null, null);
    }
    private static Modifier<List<String>, String> filter(List<List<String>> in, String... filters) {
        return modifier(in, null, 0, 0, MAX_VALUE, filters);
    }

    private static Modifier<List<String>, String> slice(List<List<String>> in, long offset, long limit) {
        return modifier(in, null, 0, offset, limit);
    }

    private static Modifier<List<String>, String> project(List<List<String>> in, Vars vars) {
        return modifier(in, vars, 0, 0, MAX_VALUE);
    }

    private static Modifier<List<String>, String> distinct(List<List<String>> in, int capacity) {
        return modifier(in, null, capacity, 0, MAX_VALUE);
    }

    private record D(Modifier<List<String>, String> plan, List<List<String>> expected) {
        void run() {
            if (!expected.isEmpty())
                assertEquals(expected.iterator().next().size(), plan.publicVars().size());
            checkRows(expected, plan.publicVars(), null, plan.execute(), true);
        }
    }

    static Stream<Arguments> test() {
        List<D> list = new ArrayList<>();
        List<List<String>> in = List.of(
                asList("_:1", "\"1\"", null),
                asList("_:2", "\"2\"", null),
                asList("_:3", "\"3\"", null),
                asList("_:4", "\"4\"", null),
                asList("_:5", "\"5\"", null)
        );
        var dupIn = new ArrayList<>(in);
        dupIn.addAll(in);
        var in1 = in.subList(0, 1);
        list.addAll(List.of(//limit & offset
                new D(slice(List.of(), 0, MAX_VALUE), List.of()),
                new D(slice(in, 0, MAX_VALUE), in),
                new D(slice(in, 1, MAX_VALUE), in.subList(1, in.size())),
                new D(slice(in, 0, 2), in.subList(0, 2)),
                new D(slice(in, 1, 2), in.subList(1, 3)),
                new D(slice(in, 2, 1), in.subList(2, 3)),
                new D(slice(in, 2, 2), in.subList(2, 4)),
                new D(slice(in, 3, 3), in.subList(3, 5))
        ));
        list.addAll(List.of(//projection
                new D(project(in1, Vars.of("x0", "x1", "x2")), in1),
                new D(project(in1, Vars.of("x0", "x1")), List.of(asList("_:1", "\"1\""))),
                new D(project(in1, Vars.of("x1", "x2")), List.of(asList("\"1\"", null))),
                new D(project(in1, Vars.of("x1")),       List.of(List.of( "\"1\""))),
                new D(project(in1, Vars.of("x1", "y")),  List.of(asList("\"1\"", null)))
        ));
        int iMax = Integer.MAX_VALUE;
        list.addAll(List.of(//distinct
                //no-op
                new D(distinct(in1, iMax), in1),
                new D(distinct(in1, 1), in1),
                new D(distinct(in, iMax), in),
                new D(distinct(in, 1), in),
                new D(distinct(in, 2), in),
                new D(distinct(dupIn, iMax), in),
                new D(distinct(dupIn, in.size()), in),
                new D(distinct(dupIn, 256), in)
        ));
        List<List<String>> numbers = List.of(
                List.of("1", "2"),
                List.of("2", "1"),
                List.of("2", "2"),
                List.of("2", "1")
        );
        list.addAll(List.of(//filters
                new D(filter(in, "?x1"), in), //non-empty strings == true
                new D(filter(in, "!bound($x2)"), in),
                new D(filter(in, "Bound(?x2)"), List.of()),

                new D(filter(numbers, "?x0 > ?x1"),
                         List.of(List.of("2", "1"), List.of("2", "1"))),
                new D(filter(numbers, "?x0 < ?x1"),
                         List.of(List.of("1", "2"))),
                new D(filter(numbers, "?x0 + ?x1 > 3"),
                        List.of(List.of("2", "2"))),
                new D(filter(numbers, "?x0 / ?x1 = ?x0"),
                         List.of(List.of("2", "1"), List.of("2", "1"))),
                new D(filter(numbers, "?x0 / ?x1 != ?x0"),
                        List.of(List.of("1", "2"), List.of("2", "2")))
        ));
        list.addAll(List.of(//filter + slice
                new D(modifier(in, null, 0, 1, 2, "?x1"),
                         in.subList(1, 3)),
                new D(modifier(in, null, 0, 1, 2, "BOUND(?x2)"),
                         List.of()),
                new D(modifier(numbers, null, 0, 1, 2, "?x0 = 2"),
                         numbers.subList(2, 4))
        ));
        list.addAll(List.of(//filter + project
                new D(modifier(in, Vars.of("x0", "x1"), 0, 0, MAX_VALUE, "?x1"),
                               in.stream().map(l -> l.subList(0, 2)).toList()),
                new D(modifier(in, Vars.of("x1"), 0, 0, MAX_VALUE, "?x1"),
                               in.stream().map(l -> l.subList(1, 2)).toList()),
                new D(modifier(in, Vars.of("y"), 0, 0, MAX_VALUE, "?x1"),
                         IntStream.range(0, in.size()).mapToObj(i -> singletonList((String) null)).toList()),

                new D(modifier(numbers, Vars.of("x1"), 0, 0, MAX_VALUE, "?x0=2"),
                        List.of(List.of("1"), List.of("2"), List.of("1")))
        ));
        list.addAll(List.of(//filter + distinct
                new D(modifier(numbers, null, iMax, 0, MAX_VALUE, "?x0=2"),
                      List.of(numbers.get(1), numbers.get(2))),
                new D(modifier(numbers, null, 3, 0, MAX_VALUE, "?x0=2"),
                        List.of(numbers.get(1), numbers.get(2))),
                new D(modifier(numbers, null, iMax, 0, MAX_VALUE, "?x1=1"),
                      List.of(numbers.get(1)))
        ));
        list.addAll(List.of(//filter + project + distinct
                new D(modifier(numbers, Vars.of("x1"), iMax, 0, MAX_VALUE, "?x0=2"),
                         List.of(List.of("1"), List.of("2"))),
                new D(modifier(numbers, Vars.of("x1", "y"), iMax, 0, MAX_VALUE, "?x0 + $x1 = 3"),
                        List.of(asList("2", null), asList("1", null)))
        ));
        list.addAll(List.of(//filter + project + distinct + slice
                new D(modifier(numbers, Vars.of("x1"), iMax, 1, MAX_VALUE, "?x0=2"),
                        List.of(List.of("2"))),
                new D(modifier(numbers, Vars.of("x1"), iMax, 0, 1, "?x0=2"),
                        List.of(List.of("1")))
        ));
        for (int i = 0; i < list.size(); i++) {
            int row = i+1;
            D d = list.get(i);
            assertFalse(d.plan.operands.get(0).publicVars().isEmpty(),
                    "bad input plan with no vars at row "+row);
            if (!d.expected.isEmpty()) {
                int expectedVars = d.expected.iterator().next().size();
                assertEquals(d.plan.publicVars().size(), expectedVars,
                             "#vars in expected rows does not match plan.publicVars() at row "+row);
            }
        }

        return list.stream().map(Arguments::arguments);
    }
    
    @ParameterizedTest @MethodSource
    void test(D c) { c.run(); }

    @RepeatedTest(3)
    void testRace() throws Exception {
        try (var tasks = new VThreadTaskSet(getClass().getSimpleName())) {
            test().map(a -> (D)a.get()[0]).forEach(d -> tasks.repeat(N_THREADS, thread -> {
                for (int i = 0; i < N_ITERATIONS; i++)
                    d.run();
            }));
        }
    }
    
}
