package com.github.alexishuf.fastersparql.operators.row;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class RowTestBase<R, I> {
    protected abstract RowType<R, I> ops();
    protected abstract I object1();

    @Test
    public void testProviderClass() {
        var ops = ops();
        R row = ops.createEmpty(Vars.of("x"));
        assertTrue(ops.rowClass.isInstance(row), row+" is not a instance of "+ops.rowClass);
    }

    public static Stream<Arguments> testSetAndGet() {
        return Stream.of(
                arguments(Vars.of("x"), 0),
                arguments(Vars.of("x", "y"), 0),
                arguments(Vars.of("x", "y"), 1),
                arguments(Vars.of("x", "y", "z"), 0),
                arguments(Vars.of("x", "y", "z"), 1),
                arguments(Vars.of("x", "y", "z"), 2)
        );
    }

    @ParameterizedTest @MethodSource
    public void testSetAndGet(Vars vars, int setIdx) {
        RowType<R, I> ops = ops();
        R row = ops.createEmpty(vars);
        for (int i = 0; i < vars.size(); i++)
            assertNull(ops.get(row, i), "i="+i);

        I value = object1();
        ops.set(row, setIdx, value);
        for (int i = 0; i < vars.size(); i++)
            assertEquals(i == setIdx ? value : null, ops.get(row, i), "i="+i);
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 2})
    public void testGetOutOfBounds(int size) {
        Vars vars = Vars.from(range(0, size).mapToObj(i -> "x" + i).toList());
        RowType<R, I> ops = ops();
        R row = ops.createEmpty(vars);
        I value = object1();
        for (int i = 0; i < size; i++)
            assertNull(ops.set(row, i, value), "i="+i);
        for (int i = 0; i < size; i++)
            assertSame(value, ops.get(row, i), "i="+i);
        assertThrows(IndexOutOfBoundsException.class, () -> ops.get(row, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> ops.get(row, size));
        assertThrows(IndexOutOfBoundsException.class, () -> ops.get(row, size+1));
    }

    @Test
    public void testGetFromNull() {
        assertNull(ops().get(null, 0));
        assertNull(ops().get(null, 1));
    }

    @Test
    public void testSetOnNull() {
        assertNull(ops().set(null, 0, object1()));
        assertNull(ops().set(null, 1, object1()));
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
        Vars vars = Vars.from(range(0, nVars).mapToObj(i -> "x" + i).toList());
        RowType<R, I> ops = ops();
        R r1 = ops.createEmpty(vars);
        R r2 = ops.createEmpty(vars);
        for (int i = 0; i < nVars; i++) {
            ops.setNT(r1, i, values1.get(i));
            ops.setNT(r2, i, values2.get(i));
        }
        assertEquals(expected, ops.equalsSameVars(r1, r2));
        assertEquals(expected, ops.equalsSameVars(r2, r1));
    }

    @Test
    public void testHash() {
        RowType<R, I> ops = ops();
        R r1a = ops.createEmpty(Vars.of("x"));
        R r1b = ops.createEmpty(Vars.of("x"));
        assertEquals(ops.hash(r1a), ops.hash(r1b));

        ops.set(r1b, 0, object1());
        assertNotEquals(ops.hash(r1a), ops.hash(r1b));

        ops.set(r1a, 0, object1());
        assertEquals(ops.hash(r1a), ops.hash(r1b));

        R r2a = ops.createEmpty(Vars.of("x", "y"));
        R r2b = ops.createEmpty(Vars.of("x", "y"));
        assertEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2a, 0, object1());
        assertNotEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2b, 0, object1());
        assertEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2a, 1, object1());
        assertNotEquals(ops.hash(r2a), ops.hash(r2b));

        ops.set(r2b, 1, object1());
        assertEquals(ops.hash(r2a), ops.hash(r2b));
    }
}