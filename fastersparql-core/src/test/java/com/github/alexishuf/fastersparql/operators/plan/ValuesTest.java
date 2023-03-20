package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.batch.type.Batch.TERM;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ValuesTest {
    static Stream<Arguments> testToString() {
        return Stream.of(
                arguments(Vars.of("x", "y"), List.of(termList("1", "2")), "Values[x, y](\n  [1, 2]\n)"),
                arguments(Vars.of("x", "y"), List.of(termList("1", "2"), termList("3", "4")), "Values[x, y](\n  [1, 2],\n  [3, 4]\n)")
        );
    }

    @ParameterizedTest @MethodSource
    void testToString(Vars vars, List<List<Term>> values, String expected) {
        var plan = FS.values(vars, values);
        assertEquals(expected, plan.toString());
    }

    record Drain(Vars vars, List<List<Term>> rows) {
        void run(BItDrainer drainer) {
            var plan = FS.values(vars, rows);
            assertEquals(vars, plan.publicVars());
            assertEquals(vars, plan.allVars());
            drainer.drainOrdered(plan.execute(TERM), rows, null);
            // iterate again, but converting the batch type
            drainer.drainOrdered(plan.execute(COMPRESSED), rows, null);
            if (rows.isEmpty()) return;

            // iterate again but write garbage to batch
            Term[] garbageRow = range(0, vars.size()).mapToObj(i -> Term.valueOf("\"garbage\""))
                                                     .toArray(Term[]::new);
            try (var it = plan.execute(TERM)) {
                TermBatch b = it.nextBatch(null);
                assertNotNull(b);
                b.clear();
                b.putRow(garbageRow);
            }

            // drain again, should not see garbage
            drainer.drainOrdered(plan.execute(TERM), rows, null);
        }
    }

    @Test
    void testDrain() {
        List<Drain> base = List.of(
                new Drain(Vars.EMPTY, List.of()),
                new Drain(Vars.of("x"), List.of()),
                new Drain(Vars.of("x", "y"), List.of()),
                new Drain(Vars.of("x"), List.of(termList("<11>"))),
                new Drain(Vars.of("x"), List.of(termList("<11>"), termList("<21>"))),
                new Drain(Vars.of("x", "y"), List.of(termList("<11>", "<12>"), termList("<21>", "<22>")))
        );
        for (BItDrainer drainer : BItDrainer.ALL) {
            for (Drain d : base)
                d.run(drainer);
        }
    }
}