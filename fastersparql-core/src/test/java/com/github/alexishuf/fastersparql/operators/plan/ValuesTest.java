package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.batch.adapters.BItDrainer;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.operators.FSOps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ValuesTest {
    static Stream<Arguments> testToString() {
        return Stream.of(
                arguments(Vars.of("x", "y"), List.of(List.of("1", "2")), "Values[x, y](\n  [1, 2]\n)"),
                arguments(Vars.of("x", "y"), List.of(List.of("1", "2"), List.of("3", "4")), "Values[x, y](\n  [1, 2],\n  [3, 4]\n)")
        );
    }

    @ParameterizedTest @MethodSource
    void testToString(Vars vars, List<List<String>> values, String expected) {
        var plan = new Values<>(ListRow.STRING, vars, values, null, null);
        assertEquals(expected, plan.toString());
    }

    record Drain(Vars vars, List<List<String>> rows) {
        void run(BItDrainer drainer) {
            var plan = FSOps.values(ListRow.STRING, vars, rows);
            assertEquals(vars, plan.publicVars());
            assertEquals(vars, plan.allVars());
            drainer.drainOrdered(plan.execute(), rows, null);
        }
    }

    @Test
    void testDrain() {
        List<Drain> base = List.of(
                new Drain(Vars.EMPTY, List.of()),
                new Drain(Vars.of("x"), List.of()),
                new Drain(Vars.of("x", "y"), List.of()),
                new Drain(Vars.of("x"), List.of(List.of("<11>"))),
                new Drain(Vars.of("x"), List.of(List.of("<11>"), List.of("<21>"))),
                new Drain(Vars.of("x", "y"), List.of(List.of("<11>", "<12>"), List.of("<21>", "<22>")))
        );
        for (BItDrainer drainer : BItDrainer.all()) {
            for (Drain d : base)
                d.run(drainer);
        }
    }
}