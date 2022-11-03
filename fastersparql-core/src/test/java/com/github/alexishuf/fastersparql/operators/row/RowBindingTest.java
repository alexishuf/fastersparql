package com.github.alexishuf.fastersparql.operators.row;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.types.ListRow;
import com.github.alexishuf.fastersparql.sparql.binding.RowBinding;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RowBindingTest {

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        return Stream.of(
                arguments(Vars.EMPTY, emptyList()),
                arguments(Vars.of("x"), singletonList("<x>")),
                arguments(Vars.of("x"), singletonList("\"23\"")),
                arguments(Vars.of("x"), singletonList("_:b1")),
                arguments(Vars.of("x", "y"), asList("<a>", "<b>")),
                arguments(Vars.of("y", "x"), asList("<b>", "<a>")),
                arguments(Vars.of("x", "y"), asList("<a>", null)),
                arguments(Vars.of("x", "y"), asList(null, "<b>")),
                arguments(Vars.of("x", "y", "z1"), asList("<a>", "<b>", "<c>")),
                arguments(Vars.of("z1", "x", "y"), asList("<c>", "<a>", "<b>")),
                arguments(Vars.of("x", "y", "z1"), asList("<a>", "<b>", null)),
                arguments(Vars.of("x", "y", "z1"), asList("<a>", null, "<c>")),
                arguments(Vars.of("x", "y", "z1"), asList(null, "<b>", "<c>"))
        );
    }

    private RowBinding<List<String>, String> wrap(Vars vars, List<String> values) {
        var b = new RowBinding<>(ListRow.STRING, vars);
        b.row(values);
        return b;
    }

    @ParameterizedTest @MethodSource("test")
    void testGet(Vars vars, List<String> values) {
        var b = wrap(vars, values);
        readBinding(b, vars, values);
    }

    private void readBinding(RowBinding<List<String>, String> b, Vars vars, List<String> values) {
        for (int i = 0; i < vars.size(); i++) {
            assertEquals(values.get(i), b.get(i), "i="+i);
            assertEquals(values.get(i), b.get(vars.get(i)), "i="+i);
        }

        assertNull(b.get("outside"));
        if (b.size() > 0) {
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> b.get(vars.size()));
        }
    }

    @ParameterizedTest @MethodSource("test")
    void testSetByIndex(Vars vars, List<String> values) {
        var b = new RowBinding<>(ListRow.STRING, vars);
        for (int i = 0; i < vars.size(); i++)
            b.set(i, values.get(i));
        readBinding(b, vars, values);
    }

    @ParameterizedTest @MethodSource("test")
    void testSetByName(Vars vars, List<String> values) {
        var b = new RowBinding<>(ListRow.STRING, vars);
        for (int i = 0; i < vars.size(); i++)
            b.set(vars.get(i), values.get(i));
        readBinding(b, vars, values);
    }
}