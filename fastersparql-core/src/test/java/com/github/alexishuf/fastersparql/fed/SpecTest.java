package com.github.alexishuf.fastersparql.fed;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;
import org.tomlj.TomlTable;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Stream.concat;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SpecTest {

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(Spec.of(), Spec.of(), true),
                arguments(Spec.of(), Spec.of("i", 7), false),
                arguments(Spec.of("i", 7), Spec.of("i", 7), true),
                arguments(Spec.of("i", 7), Spec.of("i", 8), false),
                arguments(Spec.of("i", 1, "j", 2), Spec.of("i", 1, "j", 2), true),
                arguments(Spec.of("i", 1, "j", 2), Spec.of("i", 1, "j", 2, "k", 3), false),
                arguments(Spec.of("j", 2, "i", 1), Spec.of("i", 1, "j", 2), true),
                arguments(Spec.of("j", 1, "i", 2), Spec.of("i", 1, "j", 2), false),
                arguments(Spec.of("a", Spec.of("i", 1)), Spec.of("a", Spec.of("i", 1)), true),
                arguments(Spec.of("a", Spec.of("i", 1)), Spec.of("a", Spec.of("i", 2)), false)
        );
    }

    @SuppressWarnings({"SimplifiableAssertion", "EqualsWithItself", "ConstantValue"})
    @ParameterizedTest @MethodSource
    void testEquals(Spec l, Spec r, boolean expected) {
        assertTrue(l.equals(l));
        assertTrue(r.equals(r));
        assertFalse(l.equals(null));
        assertEquals(expected, l.equals(r));
        assertEquals(expected, r.equals(l));
    }

    static Stream<Arguments> testConvertToml() {
        return Stream.of(
                arguments(Toml.parse("i=1"), Spec.of("i", 1L)),
                arguments(Toml.parse("i=1\nj=2"), Spec.of("i", 1L, "j", 2L)),
                arguments(Toml.parse("a.i=1\nj=2"), Spec.of("a", Spec.of("i", 1L), "j", 2L)),
                arguments(Toml.parse("a.i=1\na.j=2"), Spec.of("a", Spec.of("i", 1L, "j", 2L))),
                arguments(Toml.parse("a=[]"), Spec.of("a", List.of())),
                arguments(Toml.parse("a=[1, 2]"), Spec.of("a", List.of(1L, 2L)))
        );
    }

    @ParameterizedTest @MethodSource
    void testConvertToml(TomlTable table, Spec expected) {
        assertEquals(expected, new Spec(table));
    }

    @Test
    void testGetPlain() {
        Spec s = Spec.of("x", 1, "y", "bob", "z", true);
        assertEquals(1, s.get("x"));
        assertEquals("bob", s.get("y"));
        assertEquals("bob", s.getString("y"));
        assertTrue (s.getBool("z"));
        assertFalse(s.getBool("w"));

        assertEquals(1, s.get("x", Integer.class));
        assertEquals("bob", s.get("y", String.class));
        assertEquals("1", s.get("x", String.class)); // must call toString()

        assertEquals(1, s.getOr("x", 23));
        assertEquals(23, s.getOr("xx", 23));
        assertEquals("alice", s.getOr("xx", "alice"));
        assertEquals(true, s.getOr("z", false));
        assertEquals(false, s.getOr("w", false));
        assertEquals(true, s.getOr("w", true));

        assertThrows(IllegalArgumentException.class, () -> s.get("x", Boolean.class));
        assertThrows(IllegalArgumentException.class, () -> s.get("y", Integer.class));
        assertThrows(IllegalArgumentException.class, () -> s.getOr("y", 23));
    }

    static Stream<Arguments> testGetPath() {
        return Stream.of(
                arguments(Spec.of("i", 7),
                          List.of()),
                arguments(Spec.of("a", Spec.of("i", 7)),
                          List.of("a")),
                arguments(Spec.of("a", Spec.of("b", Spec.of("i", 7))),
                          List.of("a", "b")),
                arguments(Spec.of("a1", Spec.of("b22", Spec.of("c333", Spec.of("i", 7)))),
                          List.of("a1", "b22", "c333"))
        );
    }

    @ParameterizedTest @MethodSource
    void testGetPath(Spec in, List<String> path) {
        List<String> iPath = concat(path.stream(), Stream.of("i")).toList();
        List<String> nPath = concat(path.stream(), Stream.of("n")).toList();

        assertEquals(7, in.getOr(iPath, 23));
        assertEquals(7, in.get(iPath, Integer.class));
        assertEquals("7", in.getOr(iPath, "Alice"));
        assertEquals("7", in.get(iPath, String.class));

        assertEquals(23, in.getOr(nPath, 23));
        assertEquals("empty", in.getOr(nPath, "empty"));
        assertNull(in.get(nPath, Integer.class));
        assertNull(in.get(nPath, Double.class));
        assertNull(in.get(nPath, String.class));
    }

    static Stream<Arguments> testGetList() {
        return Stream.of(
                arguments(Spec.of("x", List.of()), List.of("x"), Integer.class, List.of()),
                arguments(Spec.of("x", List.of(1)), List.of("x"), Integer.class, List.of(1)),
                arguments(Spec.of("x", List.of(1, 2, 3)), List.of("x"), Integer.class, List.of(1, 2, 3)),
                arguments(Spec.of("x", asList(null, 2)), List.of("x"), Integer.class, asList(null, 2)),

                arguments(Spec.of("x", List.of("a")), List.of("x"), String.class, List.of("a")),
                arguments(Spec.of("x", List.of(1, 2)), List.of("x"), String.class, List.of("1", "2")),

                arguments(Spec.of("a", Spec.of("x", List.of(1))), List.of("a", "x"), Integer.class, List.of(1)),
                arguments(Spec.of("a", Spec.of("x", List.of(1))), List.of("a", "x"), String.class, List.of("1")),

                arguments(Spec.of("a", Spec.of("b", Spec.of("x", List.of(1)))), List.of("a", "b", "x"), Integer.class, List.of(1)),
                arguments(Spec.of("a", Spec.of("b", Spec.of("x", List.of(1)))), List.of("a", "b", "x"), String.class, List.of("1")),
                arguments(Spec.of("a", Spec.of("b", Spec.of("x", List.of("1")))), List.of("a", "b", "x"), String.class, List.of("1")),

                arguments(Spec.of("x", 1), List.of("x"), Integer.class, List.of(1)),
                arguments(Spec.of("x", 1), List.of("x"), String.class, List.of("1")),
                arguments(Spec.of("a", Spec.of("x", 1)), List.of("a", "x"), Integer.class, List.of(1)),
                arguments(Spec.of("a", Spec.of("x", 1)), List.of("a", "x"), String.class, List.of("1"))
        );
    }

    @ParameterizedTest @MethodSource
    void testGetList(Spec in, List<String> path, Class<?> cls, List<?> expected) {
        assertEquals(expected, in.getListOf(path, cls));
    }

    static Stream<Arguments> testTomlParseReverse() {
        return Stream.of(
                Spec.of("i", 7L),
                Spec.of("i", 7L, "j", 23L),
                Spec.of("a", Spec.of("i", 7L)),
                Spec.of("a", Spec.of("v", List.of())),
                Spec.of("a", Spec.of("v", List.of(1L))),
                Spec.of("a", Spec.of("v", List.of(1L, 2L, 3L))),
                Spec.of("a", List.of(Spec.of("b", 1L)),
                        "name", "alice", "ints", List.of(1L)),
                Spec.of("a", List.of(Spec.of("b", 1L),
                                     Spec.of("b", List.of(Spec.of("c", List.of(23L))))),
                        "name", "alice", "ints", List.of(1L))
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testTomlParseReverse(Spec spec) {
        String toml = spec.toToml();
        TomlParseResult result = Toml.parse(toml);
        assertFalse(result.hasErrors());
        assertEquals(spec, new Spec(result));
    }

}