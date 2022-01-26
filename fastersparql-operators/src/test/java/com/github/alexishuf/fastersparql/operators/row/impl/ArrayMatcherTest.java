package com.github.alexishuf.fastersparql.operators.row.impl;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ArrayMatcherTest {
    protected ArrayMatcher create(List<String> leftVars, List<String> rightVars) {
        return new ArrayMatcher(leftVars, rightVars);
    }

    protected Object toRow(List<String> list) {
        return list.toArray(new String[0]);
    }

    static Stream<Arguments> testHasIntersection() {
        return Stream.of(
            arguments(emptyList(), emptyList(), false),
            arguments(singletonList("x"), emptyList(), false),
            arguments(singletonList("x"), singletonList("x"), true),
            arguments(singletonList("x"), asList("x", "y"), true),
            arguments(singletonList("x"), asList("y", "x"), true),
            arguments(singletonList("x"), asList("z", "y"), false),
            arguments(asList("x", "y"), asList("x", "y"), true),
            arguments(asList("x", "y"), asList("x", "z"), true),
            arguments(asList("x", "y"), asList("z", "y"), true),
            arguments(asList("x", "y"), asList("z", "w"), false)
        );
    }

    @ParameterizedTest @MethodSource
    void testHasIntersection(List<String> left, List<String> right, boolean expected) {
        assertEquals(expected, create(left, right).hasIntersection());
        assertEquals(expected, create(right, left).hasIntersection());
    }

    static Stream<Arguments> testMatches() {
        return Stream.of(
            arguments(emptyList(), emptyList(), emptyList(), emptyList(), true),
            arguments(singletonList("x"), emptyList(), singletonList("1"), emptyList(), true),
            arguments(singletonList("x"), singletonList("x"),
                      singletonList("1"), singletonList("1"), true),
            arguments(singletonList("x"), singletonList("x"),
                      singletonList("1"), singletonList("2"), false),
            arguments(asList("x", "y"), asList("x", "y"),
                      asList("1", "2"), asList("1", "2"), true),
            arguments(asList("x", "y"), asList("x", "y"),
                      asList("1", "2"), asList("2", "2"), false),
            arguments(asList("x", "y"), asList("x", "y"),
                      asList("1", "2"), asList("1", "1"), false),
            arguments(asList("x", "y"), singletonList("x"),
                      asList("1", "2"), singletonList("1"), true),
            arguments(asList("x", "y"), singletonList("x"),
                      asList("1", "2"), singletonList("2"), false)
        );
    }

    @ParameterizedTest @MethodSource
    void testMatches(List<String> leftVars, List<String> rightVars,
                     List<String> leftRow, List<String> rightRow, boolean expected) {
        ArrayMatcher matcher = create(leftVars, rightVars);
        assertEquals(expected, matcher.matches(toRow(leftRow), toRow(rightRow)));

        ArrayMatcher flipped = create(rightVars, leftVars);
        assertEquals(expected, flipped.matches(toRow(rightRow), toRow(leftRow)));
    }

}