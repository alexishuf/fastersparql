package com.github.alexishuf.fastersparql.operators.row;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.expr.Term.termList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RowBindingTest {

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        return Stream.of(
                arguments(Vars.EMPTY, termList()),
                arguments(Vars.of("x"), termList("<x>")),
                arguments(Vars.of("x"), termList("\"23\"")),
                arguments(Vars.of("x"), termList("_:b1")),
                arguments(Vars.of("x", "y"), termList("<a>", "<b>")),
                arguments(Vars.of("y", "x"), termList("<b>", "<a>")),
                arguments(Vars.of("x", "y"), termList("<a>", null)),
                arguments(Vars.of("x", "y"), termList(null, "<b>")),
                arguments(Vars.of("x", "y", "z1"), termList("<a>", "<b>", "<c>")),
                arguments(Vars.of("z1", "x", "y"), termList("<c>", "<a>", "<b>")),
                arguments(Vars.of("x", "y", "z1"), termList("<a>", "<b>", null)),
                arguments(Vars.of("x", "y", "z1"), termList("<a>", null, "<c>")),
                arguments(Vars.of("x", "y", "z1"), termList(null, "<b>", "<c>"))
        );
    }

    @ParameterizedTest @MethodSource("test")
    void testGet(Vars vars, List<Term> values) {
        readBinding(new RowBinding<>(RowType.LIST, vars).row(values), vars, values);
    }

    private void readBinding(RowBinding<List<Term>> b, Vars vars, List<Term> values) {
        for (int i = 0; i < vars.size(); i++) {
            assertEquals(values.get(i), b.get(i), "i="+i);
            assertEquals(values.get(i), b.get(vars.get(i)), "i="+i);
        }
        assertNull(b.get(Rope.of("outside")));
        assertNull(b.get(Term.valueOf("?outside")));
        if (b.size() > 0) {
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(vars.size()));
        }
    }

    @ParameterizedTest @MethodSource("test")
    void testSetByIndex(Vars vars, List<Term> values) {
        var b = new RowBinding<>(RowType.LIST, vars);
        for (int i = 0; i < vars.size(); i++)
            b.set(i, values.get(i));
        readBinding(b, vars, values);
    }

    @ParameterizedTest @MethodSource("test")
    void testSetByName(Vars vars, List<Term> values) {
        var b = new RowBinding<>(RowType.LIST, vars);
        for (int i = 0; i < vars.size(); i++)
            b.set(vars.get(i), values.get(i));
        readBinding(b, vars, values);
    }
}