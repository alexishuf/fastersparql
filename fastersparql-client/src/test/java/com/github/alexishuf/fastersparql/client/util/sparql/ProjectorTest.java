package com.github.alexishuf.fastersparql.client.util.sparql;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ProjectorTest {
    static Stream<Arguments> test() {
        return Stream.of(
                arguments(singletonList("x"), singletonList("x"),
                          singletonList(1), singletonList(1)),
                arguments(singletonList("x"), singletonList("y"),
                          singletonList(1), singletonList(null)),

                arguments(asList("x", "y"), asList("x", "y"), asList(1, 2), asList(1, 2)),
                arguments(asList("x", "y"), asList("x", "y"), asList(1, null), asList(1, null)),
                arguments(asList("x", "y"), asList("x", "z"), asList(1, 2), asList(1, null)),
                arguments(asList("x", "y"), asList("x", "z"), asList(null, 2), asList(null, null)),
                arguments(asList("x", "y"), asList("x", "z"),
                          asList(null, null), asList(null, null)),

                arguments(asList("x", "y"), asList("z", "w"), asList(1, 2), asList(null, null)),
                arguments(asList("x", "y"), singletonList("x"), singletonList(1), asList(1, null)),
                arguments(asList("x", "y"), singletonList("y"), singletonList(1), asList(null, 1))
        );
    }

    @ParameterizedTest @MethodSource("test")
    void test(List<String> outVars, List<String> inVars, List<@Nullable Integer> in,
                   List<@Nullable Integer> expected) {
        Projector projector = Projector.createFor(outVars, inVars);
        List<@Nullable Integer> inCopy = new ArrayList<>(in);
        List<@Nullable Integer> out = projector.project(in);
        assertEquals(expected, out);
        assertEquals(inCopy, in);
    }

    @ParameterizedTest @MethodSource("test")
    void testArray(List<String> outVars, List<String> inVars, List<@Nullable Integer> inList,
              List<@Nullable Integer> expectedList) {
        @Nullable Integer[] in = inList.toArray(new Integer[0]);
        @Nullable Integer[] expected = expectedList.toArray(new Integer[0]);

        Projector projector = Projector.createFor(outVars, inVars);
        @Nullable Integer[] out = projector.project(in);
        assertArrayEquals(expected, out);
        assertArrayEquals(inList.toArray(new Integer[0]), in);
    }

}