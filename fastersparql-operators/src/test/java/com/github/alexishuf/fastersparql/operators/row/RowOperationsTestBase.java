package com.github.alexishuf.fastersparql.operators.row;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class RowOperationsTestBase {
    protected abstract RowOperationsProvider provider();
    protected abstract Object object1();

    @Test
    public void testProviderClass() {
        RowOperations ops = provider().get(provider().rowClass());
        Object row = ops.createEmpty(singletonList("x"));
        Class<?> cls = provider().rowClass();
        assertTrue(cls.isInstance(row), row+" is not a instance of "+cls);
    }

    public static Stream<Arguments> testSetAndGet() {
        return Stream.of(
                arguments(singletonList("x"), 0),
                arguments(asList("x", "y"), 0),
                arguments(asList("x", "y"), 1),
                arguments(asList("x", "y", "z"), 0),
                arguments(asList("x", "y", "z"), 1),
                arguments(asList("x", "y", "z"), 2)
        );
    }

    @ParameterizedTest @MethodSource
    public void testSetAndGet(List<String> vars, int setIdx) {
        RowOperations ops = provider().get(provider().rowClass());
        Object row = ops.createEmpty(vars);
        for (int i = 0; i < vars.size(); i++)
            assertNull(ops.get(row, i, vars.get(i)), "i="+i);

        Object value = object1();
        ops.set(row, setIdx, vars.get(setIdx), value);
        for (int i = 0; i < vars.size(); i++)
            assertEquals(i == setIdx ? value : null, ops.get(row, i, vars.get(i)), "i="+i);
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2})
    public void testGetOutOfBounds(int size) {
        List<String> vars = IntStream.range(0, size).mapToObj(i -> "x" + i).collect(toList());
        RowOperations ops = provider().get(provider().rowClass());
        Object row = ops.createEmpty(vars);
        Object value = object1();
        for (int i = 0; i < size; i++)
            assertNull(ops.set(row, i, vars.get(i), value), "i="+i);
        for (int i = 0; i < size; i++)
            assertSame(value, ops.get(row, i, vars.get(i)), "i="+i);
        assertThrows(IndexOutOfBoundsException.class, () -> ops.get(row, -1, "y"));
        assertThrows(IndexOutOfBoundsException.class, () -> ops.get(row, size, "y"));
        assertThrows(IndexOutOfBoundsException.class, () -> ops.get(row, size+1, "y"));
    }

    @Test
    public void testGetFromNull() {
        assertNull(provider().get(provider().rowClass()).get(null, 0, "x"));
    }

    @Test
    public void testSetOnNull() {
        assertNull(provider().get(provider().rowClass()).set(null, 0, "x", object1()));
    }

    @SuppressWarnings("unused") static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(emptyList(), emptyList(), true),
                arguments(singletonList("<x>"), singletonList("<x>"), true),
                arguments(asList("<x>", "<y>"), asList("<x>", "<y>"), true),
                arguments(singletonList("<x>"), singletonList("<x1>"), false),
                arguments(asList("<x>", null), asList("<x>", "<x1>"), false),
                arguments(asList("<x>", "<y1>"), asList("<x>", "<y2>"), false),
                arguments(asList("<x1>", "<y>"), asList("<x2>", "<y>"), false),
                arguments(asList(null, null), asList("<x>", "<y>"), false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testEquals(List<String> values1, List<String> values2, boolean expected) {
        int nVars = values1.size();
        assertEquals(nVars, values2.size(), "bad test data");
        List<String> vars = IntStream.range(0, nVars).mapToObj(i -> "x" + i).collect(toList());
        RowOperations ops = provider().get(provider().rowClass());
        Object r1 = ops.createEmpty(vars);
        Object r2 = ops.createEmpty(vars);
        for (int i = 0; i < nVars; i++) {
            ops.set(r1, i, "x"+i, values1.get(i));
            ops.set(r2, i, "x"+i, values2.get(i));
        }
        assertEquals(expected, ops.equalsSameVars(r1, r2));
        assertEquals(expected, ops.equalsSameVars(r2, r1));
    }

    @Test
    public void testHash() {
        RowOperations ops = provider().get(provider().rowClass());
        Object r1a = ops.createEmpty(singletonList("x"));
        Object r1b = ops.createEmpty(singletonList("x"));
        assertEquals(ops.hash(r1a), ops.hash(r1b));

        ops.set(r1b, 0, "x", object1());
        assertNotEquals(ops.hash(r1a), ops.hash(r1b));

        ops.set(r1a, 0, "x", object1());
        assertEquals(ops.hash(r1a), ops.hash(r1b));

        Object r2a = ops.createEmpty(asList("x", "y"));
        Object r2b = ops.createEmpty(asList("x", "y"));
        assertEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2a, 0, "x", object1());
        assertNotEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2b, 0, "x", object1());
        assertEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2a, 1, "y", object1());
        assertNotEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2b, 1, "y", object1());
        assertEquals(ops.hash(r2a), ops.hash(r2b));
    }
}