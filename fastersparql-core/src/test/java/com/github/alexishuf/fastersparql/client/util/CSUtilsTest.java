package com.github.alexishuf.fastersparql.client.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CSUtilsTest {
    static Stream<Arguments> testStartsWith() {
        return Stream.of(
                arguments("http://asd", 0, "http://", true),
                arguments("http://asd", 0, "https://", false),
                arguments("http://asd", 0, "http:/", true),
                arguments("http://asd", 0, "h", true),
                arguments("http://asd", 0, "", true),
                arguments("", 0, "", true),
                arguments("https://asd", 0, "https://", true),
                arguments("https://asd", 0, "http://", false),
                arguments("https://asd", 0, "http:", false),
                arguments("https://asd", 0, "http", true),
                arguments("https://asd", 0, "asd", false),
                arguments("https://asd", 0, "://", false),
                arguments("https://asd", 0, "/", false),
                arguments(" asd", 0, "as", false),
                arguments(" asd", 1, "as", true),
                arguments(" asd", 2, "as", false),
                arguments("aasd", 0, "as", false),
                arguments("aasd", 1, "as", true),
                arguments("aasd", 2, "as", false),
                arguments("asd", 0, "as", true)
        );
    }

    @ParameterizedTest @MethodSource
    void testStartsWith(String cs, int begin, String prefix, boolean expected) {
        assertEquals(expected, CSUtils.startsWith(cs, begin, prefix));
        assertEquals(expected, CSUtils.startsWith(new StringBuilder(cs), begin, prefix));
    }

    static Stream<Arguments> testFindNotEscaped() {
        return Stream.of(
                arguments("", 0, '"', 0),
                arguments("a", 0, '"', 1),
                arguments("a", 1, '"', 1),
                arguments("aax", 0, 'x', 2),
                arguments("aax", 1, 'x', 2),
                arguments("aax", 2, 'x', 2),
                arguments("aax", 3, 'x', 3),
                arguments("ax", 0, 'x', 1),
                arguments("ax", 1, 'x', 1),
                arguments("ax", 2, 'x', 2),
                arguments("x", 0, 'x', 0),
                arguments("x", 1, 'x', 1),
                arguments("aa\\xx", 0, 'x', 4),
                arguments("aa\\xx", 1, 'x', 4),
                arguments("aa\\xx", 2, 'x', 4),
                arguments("aa\\xx", 3, 'x', 4),
                arguments("aa\\xx", 4, 'x', 4),
                arguments("aa\\xx", 5, 'x', 5),
                arguments("a\\xx", 0, 'x', 3),
                arguments("a\\xx", 1, 'x', 3),
                arguments("a\\xx", 2, 'x', 3),
                arguments("a\\xx", 3, 'x', 3),
                arguments("a\\xx", 4, 'x', 4),
                arguments("\\xx", 0, 'x', 2),
                arguments("\\xx", 1, 'x', 2),
                arguments("\\xx", 2, 'x', 2),
                arguments("\\xx", 3, 'x', 3),
                arguments("\\x", 0, 'x', 2),
                arguments("\\x", 1, 'x', 2)
        );
    }

    @ParameterizedTest @MethodSource
    void testFindNotEscaped(String string, int from, char ch, int expected) {
        for (CharSequence cs : asList(string, new StringBuilder(string))) {
            assertEquals(expected, CSUtils.findNotEscaped(cs, from, ch),
                         "cs.getClass()="+cs.getClass());
        }
    }

    @ParameterizedTest @ValueSource(strings = {
            "",
            "a",
            "ação",
            "...\uD83E\uDE02___"
    })
    void testHash(String string) {
        assertEquals(string.hashCode(), CSUtils.hash(string));
        assertEquals(string.hashCode(), CSUtils.hash(new StringBuilder(string)));

        CharSequence subSequence = new StringBuilder(">" + string + "<")
                .subSequence(1, 1 + string.length());
        assertEquals(string.hashCode(), CSUtils.hash(subSequence));
    }
}