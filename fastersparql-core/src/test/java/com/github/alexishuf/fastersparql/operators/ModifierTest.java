package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.util.TestTaskSet;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Modifier;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprParser;
import com.github.alexishuf.fastersparql.util.Results;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.Results.results;
import static java.lang.Long.MAX_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ModifierTest {
    private static final int N_ITERATIONS = 16;
    private static final int N_THREADS = 2;

    private static Modifier modifier(Results in, Vars projection,
                                                 int distinctCapacity, long offset, long limit,
                                                 CharSequence... filters) {
        assertNotEquals(0, limit, "nonsense");
        ExprParser p = new ExprParser();

        List<Expr> expressions = new ArrayList<>(filters.length);
        for (var string : filters)
            expressions.add(p.parse(SegmentRope.of(string)));
        return new Modifier(in.asPlan(), projection, distinctCapacity,
                              offset, limit, expressions);
    }
    private static Modifier filter(Results in, String... filters) {
        return modifier(in, null, 0, 0, MAX_VALUE, filters);
    }

    private static Modifier slice(Results in, long offset, long limit) {
        return modifier(in, null, 0, offset, limit);
    }

    private static Modifier project(Results in, Vars vars) {
        return modifier(in, vars, 0, 0, MAX_VALUE);
    }

    private static Modifier distinct(Results in, int capacity) {
        return modifier(in, null, capacity, 0, MAX_VALUE);
    }

    private record D(Modifier plan, Results expected) {
        void run() {
            if (!expected.isEmpty())
                assertEquals(expected.columns(), plan.publicVars().size());
            expected.check(plan.execute(Batch.TERM));
            expected.check(plan.execute(Batch.TERM));
            expected.check(plan.execute(Batch.COMPRESSED));
            expected.check(plan.emit(Batch.TERM, Vars.EMPTY));
            expected.check(plan.emit(Batch.TERM, Vars.EMPTY));
            expected.check(plan.emit(Batch.COMPRESSED, Vars.EMPTY));
            expected.check(plan.emit(Batch.TERM, Vars.EMPTY));
            expected.check(plan.emit(Batch.COMPRESSED, Vars.EMPTY));
        }
    }

    static Stream<Arguments> test() {
        List<D> list = new ArrayList<>();
        Results in = results("?x0 ?x1 ?x2",
                "_:1", "\"1\"", null,
                "_:2", "\"2\"", null,
                "_:3", "\"3\"", null,
                "_:4", "\"4\"", null,
                "_:5", "\"5\"", null);
        var dupIn = results(in.vars(),
                Stream.concat(in.expected().stream(), in.expected().stream()).toList());
        var in1 = in.sub(0, 1);
        list.addAll(List.of(//limit & offset
                new D(slice(results(), 0, MAX_VALUE), results()),
                new D(slice(in, 0, MAX_VALUE), in),
                new D(slice(in, 1, MAX_VALUE), in.sub(1, in.size())),
                new D(slice(in, 0, 2), in.sub(0, 2)),
                new D(slice(in, 1, 2), in.sub(1, 3)),
                new D(slice(in, 2, 1), in.sub(2, 3)),
                new D(slice(in, 2, 2), in.sub(2, 4)),
                new D(slice(in, 3, 3), in.sub(3, 5))
        ));
        list.addAll(List.of(//projection
                new D(project(in1, Vars.of("x0", "x1", "x2")), in1),
                new D(project(in1, Vars.of("x0", "x1")), results("?x0 ?x1", "_:1", "\"1\"")),
                new D(project(in1, Vars.of("x1", "x2")), results("?x1 ?x2", "\"1\"", null)),
                new D(project(in1, Vars.of("x1")),       results("?x1",  "\"1\"")),
                new D(project(in1, Vars.of("x1", "y")),  results("?x1 ?y", "\"1\"", null))
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
                new D(distinct(dupIn, FSProperties.reducedCapacity()), in),
                new D(distinct(dupIn, FSProperties.distinctCapacity()), in)
        ));
        var numbers = results("?x0 ?x1",
                "1", "2",
                "2", "1",
                "2", "2",
                "2", "1"
        );
        list.addAll(List.of(//filters
                new D(filter(in, "?x1"), in), //non-empty strings == true
                new D(filter(in, "!bound($x2)"), in),
                new D(filter(in, "Bound(?x2)"), results(in.vars())),

                new D(filter(numbers, "?x0 > ?x1"),
                        results("?x0 ?x1", "2", "1", "2", "1")),
                new D(filter(numbers, "?x0 < ?x1"),
                        results("?x0 ?x1", "1", "2")),
                new D(filter(numbers, "?x0 + ?x1 > 3"),
                        results("?x0 ?x1", "2", "2")),
                new D(filter(numbers, "?x0 / ?x1 = ?x0"),
                        results("?x0 ?x1", "2", "1", "2", "1")),
                new D(filter(numbers, "?x0 / ?x1 != ?x0"),
                        results("?x0 ?x1", "1", "2", "2", "2"))
        ));
        list.addAll(List.of(//filter + slice
                new D(modifier(in, null, 0, 1, 2, "?x1"),
                         in.sub(1, 3)),
                new D(modifier(in, null, 0, 1, 2, "BOUND(?x2)"),
                         results(in.vars())),
                new D(modifier(numbers, null, 0, 1, 2, "?x0 = 2"),
                         numbers.sub(2, 4))
        ));
        list.addAll(List.of(//filter + project
                new D(modifier(in, Vars.of("x0", "x1"), 0, 0, MAX_VALUE, "?x1"),
                      in.projection(0, 1)),
                new D(modifier(in, Vars.of("x1"), 0, 0, MAX_VALUE, "?x1"),
                      in.projection(1)),
                new D(modifier(in, Vars.of("y"), 0, 0, MAX_VALUE, "?x1"),
                      results("?y", null, null, null, null, null)),

                new D(modifier(numbers, Vars.of("x1"), 0, 0, MAX_VALUE, "?x0=2"),
                      results("?x1", "1", "2", "1"))
        ));
        list.addAll(List.of(//filter + distinct
                new D(modifier(numbers, null, iMax, 0, MAX_VALUE, "?x0=2"),
                      numbers.sub(1, 3)),
                new D(modifier(numbers, null, 3, 0, MAX_VALUE, "?x0=2"),
                      numbers.sub(1, 3)),
                new D(modifier(numbers, null, iMax, 0, MAX_VALUE, "?x1=1"),
                      numbers.sub(1, 2))
        ));
        list.addAll(List.of(//filter + project + distinct
                new D(modifier(numbers, Vars.of("x1"), iMax, 0, MAX_VALUE, "?x0=2"),
                      results("?x1", "1", "2")),
                new D(modifier(numbers, Vars.of("x1", "y"), iMax, 0, MAX_VALUE, "?x0 + $x1 = 3"),
                      results("?x1 ?y", "2", null, "1", null))
        ));
        list.addAll(List.of(//filter + project + distinct + slice
                new D(modifier(numbers, Vars.of("x1"), iMax, 1, MAX_VALUE, "?x0=2"),
                      results("?x1", "2")),
                new D(modifier(numbers, Vars.of("x1"), iMax, 0, 1, "?x0=2"),
                      results("?x1", "1"))
        ));
        for (int i = 0; i < list.size(); i++) {
            int row = i+1;
            D d = list.get(i);
            assertEquals(d.expected.vars().size(), d.plan.publicVars().size(),
                    "#vars for modifier does not match expected results #vars at row "+row);
            assertEquals(d.expected.vars(), d.plan.publicVars(),
                    "var names/ordering for expected and modifier do not match at row "+row);
        }

        return list.stream().map(Arguments::arguments);
    }
    
    @ParameterizedTest @MethodSource
    void test(D c) {
        try (var w = ThreadJournal.watchdog(System.out, 100)) {
            ThreadJournal.closeThreadJournals();
            w.start(1_000_000_000L);
            c.run();
        } catch (Throwable t) {
            ThreadJournal.dumpAndReset(System.out, 60);
            throw t;
        }
    }

    @Test void testRace() throws Exception {
        try (var tasks = TestTaskSet.platformTaskSet(getClass().getSimpleName())) {
            for (var it = test().map(a -> (D)a.get()[0]).iterator(); it.hasNext(); ) {
                D d = it.next();
                tasks.repeat(N_THREADS, () -> {
                    for (int i = 0; i < N_ITERATIONS; i++) d.run();
                });
                tasks.await();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ModifierTest test = new ModifierTest();
        D first = (D) test().findFirst().orElseThrow().get()[0];
        first.run();
        try (var rec = new Recording(Configuration.getConfiguration("profile"))) {
            rec.setDumpOnExit(true);
            rec.setDestination(Path.of("/tmp/profile.jfr"));
            rec.start();
            test.testRace();
            test.testRace();
            test.testRace();
            test.testRace();
            test().map(a -> (D)a.get()[0]).forEach(test::test);
        }
    }

}
