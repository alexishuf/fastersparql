package com.github.alexishuf.fastersparql.operators.row;

import com.github.alexishuf.fastersparql.client.model.row.RowBinding;
import com.github.alexishuf.fastersparql.client.model.row.impl.ListOperations;
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
                arguments(emptyList(), emptyList()),
                arguments(singletonList("x"), singletonList("<x>")),
                arguments(singletonList("x"), singletonList("\"23\"")),
                arguments(singletonList("x"), singletonList("_:b1")),
                arguments(asList("x", "y"), asList("<a>", "<b>")),
                arguments(asList("y", "x"), asList("<b>", "<a>")),
                arguments(asList("x", "y"), asList("<a>", null)),
                arguments(asList("x", "y"), asList(null, "<b>")),
                arguments(asList("x", "y", "z1"), asList("<a>", "<b>", "<c>")),
                arguments(asList("z1", "x", "y"), asList("<c>", "<a>", "<b>")),
                arguments(asList("x", "y", "z1"), asList("<a>", "<b>", null)),
                arguments(asList("x", "y", "z1"), asList("<a>", null, "<c>")),
                arguments(asList("x", "y", "z1"), asList(null, "<b>", "<c>"))
        );
    }

    private RowBinding<List<String>> wrap(List<String> vars, List<String> values) {
        RowBinding<List<String>> b = new RowBinding<>(ListOperations.get(), vars);
        b.row(values);
        return b;
    }

    @ParameterizedTest @MethodSource("test")
    void testVars(List<String> vars, List<String> values) {
        RowBinding<List<String>> b = wrap(vars, values);
        int size = vars.size();
        assertEquals(size, b.size());
        for (int i = 0; i < size; i++) {
            assertEquals(vars.get(i), b.var(i), "i="+i);
            assertTrue(b.contains(vars.get(i)));
            assertEquals(i, b.indexOf(vars.get(i)));
        }
        assertThrows(IndexOutOfBoundsException.class, () -> b.var(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> b.var(size));

        assertFalse(b.contains("outside"));

        assertEquals(-1, b.indexOf("outside"));
    }

    @ParameterizedTest @MethodSource("test")
    void testGet(List<String> vars, List<String> values) {
        RowBinding<List<String>> b = wrap(vars, values);
        readBinding(b, vars, values);
    }

    private void readBinding(RowBinding<List<String>> b, List<String> vars, List<String> values) {
        for (int i = 0; i < vars.size(); i++) {
            assertEquals(values.get(i), b.get(i), "i="+i);
            assertEquals(values.get(i), b.get(vars.get(i)), "i="+i);
        }

        assertNull(b.get("outside"));
        assertThrows(IndexOutOfBoundsException.class, () -> b.get(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> b.get(vars.size()));
    }

    @ParameterizedTest @MethodSource("test")
    void testSetByIndex(List<String> vars, List<String> values) {
        RowBinding<List<String>> b = new RowBinding<>(ListOperations.get(), vars);
        for (int i = 0; i < vars.size(); i++)
            b.set(i, values.get(i));
        readBinding(b, vars, values);
    }

    @ParameterizedTest @MethodSource("test")
    void testSetByName(List<String> vars, List<String> values) {
        RowBinding<List<String>> b = new RowBinding<>(ListOperations.get(), vars);
        for (int i = 0; i < vars.size(); i++)
            b.set(vars.get(i), values.get(i));
        readBinding(b, vars, values);
    }
}