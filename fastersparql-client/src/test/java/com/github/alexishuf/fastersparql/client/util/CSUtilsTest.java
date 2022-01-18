package com.github.alexishuf.fastersparql.client.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CSUtilsTest {
    @ParameterizedTest @ValueSource(strings = {
            "bob | 0 | o | a | 1",
            "bob | 0 | a | o | 1",
            "bob | 1 | a | o | 1",
            "bob | 0 | b | a | 0",
            "bob | 1 | b | a | 2",
            "bob | 2 | b | a | 2",
            "bob | 2 | a | b | 2",
            "bob | 3 | b | a | 3",
            "bob | 5 | b | a | 3",
            "bob | 0 | x | y | 3",
            "bob | 2 | x | y | 3",
            "bob | 3 | x | y | 3",
            "\uD83E\uDE03abc | 0 | a | b | 2",
            "\uD83E\uDE03abc | 0 | b | a | 2",
            "\uD83E\uDE03abc | 2 | a | b | 2",
            "\uD83E\uDE03abc | 3 | a | b | 3",
            "\uD83E\uDE03abc | 4 | a | b | 5",
            "\uD83E\uDE03abc | 0 | a      | \uD83E | 0",
            "\uD83E\uDE03abc | 0 | \uD83E | a      | 0",
            "\uD83E\uDE03abc | 1 | \uD83E | a      | 2",
            "\uD83E\uDE03abc | 2 | \uD83E | a      | 2",
            "\uD83E\uDE03abc | 3 | \uD83E | a      | 5",
            "\uD83E\uDE03abc | 0 | a      | \uDE03 | 1",
            "\uD83E\uDE03abc | 0 | \uDE03 | a      | 1",
            "\uD83E\uDE03abc | 1 | a      | \uDE03 | 1",
            "\uD83E\uDE03abc | 1 | \uDE03 | a      | 1",
            "\uD83E\uDE03abc | 2 | \uDE03 | a      | 2",
            "\uD83E\uDE03abc | 2 | \uDE03 | x      | 5",
            "\uD83E\uDE03abc | 3 | \uDE03 | a      | 5",
            "?x\t?y\n        | 0 | \t     | \n     | 2",
            "?x\t?y\n        | 0 | \n     | \t     | 2",
            "?x\t?y\n        | 2 | \t     | \n     | 2",
            "?x\t?y\n        | 2 | \n     | \t     | 2",
            "?x\t?y\n        | 3 | \t     | \n     | 5",
            "?x\t?y\n        | 3 | \n     | \t     | 5",
            "?x\t?y\n        | 5 | \t     | \n     | 5",
            "?x\t?y\n        | 5 | \n     | \t     | 5",
    })
    void testSkipUntilEither(String dataString) {
        String[] args = dataString.split(" *\\| *");
        String string = args[0];
        String longString = args[0] + IntStream.range(1, 32).mapToObj(Integer::toString)
                                                .collect(Collectors.joining(""));
        int from = parseInt(args[1]), shortExpected = parseInt(args[4]);
        char ch0 = args[2].charAt(0), ch1 = args[3].charAt(0);
        for (CharSequence input : asList(string, new StringBuilder(string), longString)) {
            int expected = shortExpected == string.length() ? input.length() : shortExpected;
            assertEquals(expected, CSUtils.skipUntil(input, from, ch0, ch1),
                         "input.getClass()="+input.getClass().getSimpleName());
        }
    }

    @ParameterizedTest @ValueSource(strings = {
            "x | 0 | 0 | x | 0",
            "x | 0 | 1 | x | 0",

            "0x | 0 | 2 | x | 1",
            "0x | 1 | 2 | x | 1",
            "0x | 2 | 2 | x | 2",

            "0x1x | 0 | 4 | x | 1",
            "0x1x | 0 | 2 | x | 1",
            "0x1x | 0 | 1 | x | 1",
            "0x1x | 2 | 4 | x | 3",
            "0x1x | 2 | 3 | x | 3",

            "0x12x | 0 | 5 | x | 1",
            "0x12x | 1 | 5 | x | 1",
            "0x12x | 2 | 5 | x | 4",
            "0x12x | 2 | 4 | x | 4",
            "0x12x | 2 | 3 | x | 3",
            "0x12x | 2 | 2 | x | 2",
    })
    void testSkipUntilIn(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int from = parseInt(data[1]), end = parseInt(data[2]), expected = parseInt(data[4]);
        char c = data[3].charAt(0);
        for (CharSequence cs : asList(data[0], new StringBuilder(data[0])))
            assertEquals(expected, CSUtils.skipUntilIn(cs, from, end, c));
    }

    @ParameterizedTest @ValueSource(strings = {
            "012, | 0 | ,}] | 3",
            "012] | 0 | ,}] | 3",
            "012} | 0 | ,}] | 3",
            "012, | 1 | ,}] | 3",
            "012] | 1 | ,}] | 3",
            "012} | 1 | ,}] | 3",
            "012, | 2 | ,}] | 3",
            "012] | 2 | ,}] | 3",
            "012} | 2 | ,}] | 3",
            "012, | 3 | ,}] | 3",
            "012] | 3 | ,}] | 3",
            "012} | 3 | ,}] | 3",
            "012, | 4 | ,}] | 4",
            "012] | 4 | ,}] | 4",
            "012} | 4 | ,}] | 4",
            "012  | 0 | ,}] | 3",
            "012,x | 0 | ,}] | 3",
            "012,x | 1 | ,}] | 3",
            "012,x | 2 | ,}] | 3",
            "012,x | 3 | ,}] | 3",
            "012,x | 4 | ,}] | 5",
            "012]x | 0 | ,}] | 3",
            "012]x | 1 | ,}] | 3",
            "012]x | 2 | ,}] | 3",
            "012]x | 3 | ,}] | 3",
            "012]x | 4 | ,}] | 5",
    })
    void testSkipUntilThree(String dataString) {
        String[] data = dataString.split(" *\\| *");
        String input = data[0];
        int from = Integer.parseInt(data[1]), expected = Integer.parseInt(data[3]);
        char c0 = data[2].charAt(0), c1 = data[2].charAt(1), c2 = data[2].charAt(2);
        for (CharSequence cs : asList(input, new StringBuilder(input))) {
            assertEquals(expected, CSUtils.skipUntil(cs, from, c0, c1, c2),
                         "cs.getClass()="+cs.getClass());
        }
    }

    @ParameterizedTest @ValueSource(strings = {
            "0x12x3 | 0 | 6 | 1",
            "0x12x3 | 1 | 6 | 1",
            "0x12x3 | 2 | 6 | 4",
            "0x12x3 | 4 | 6 | 4",
            "0x12x3 | 5 | 6 | 6",
            "0x12x3 | 0 | 2 | 1",
            "0x12x3 | 1 | 2 | 1",
            "0x12x3 | 2 | 2 | 2",

            "0y12y3 | 0 | 6 | 1",
            "0y12y3 | 1 | 6 | 1",
            "0y12y3 | 2 | 6 | 4",
            "0y12y3 | 4 | 6 | 4",
            "0y12y3 | 5 | 6 | 6",
            "0y12y3 | 0 | 2 | 1",
            "0y12y3 | 1 | 2 | 1",
            "0y12y3 | 2 | 2 | 2",

            "0z12z3 | 0 | 6 | 1",
            "0z12z3 | 1 | 6 | 1",
            "0z12z3 | 2 | 6 | 4",
            "0z12z3 | 4 | 6 | 4",
            "0z12z3 | 5 | 6 | 6",
            "0z12z3 | 0 | 2 | 1",
            "0z12z3 | 1 | 2 | 1",
            "0z12z3 | 2 | 2 | 2",
    })
    void testSkipUntilInThree(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int from = parseInt(data[1]), end = parseInt(data[2]), expected = parseInt(data[3]);
        for (CharSequence cs : asList(data[0], new StringBuilder(data[0])))
            assertEquals(expected, CSUtils.skipUntilIn(cs, from, end, 'x', 'y', 'z'));
    }

    @ParameterizedTest @ValueSource(strings = {
            "",
            "a",
            "ab",
            "abc",
            "abcdefghijklm",
    })
    void testPositiveSkipUntil(String string) {
        for (CharSequence cs : asList(string, new StringBuilder(string))) {
            for (int i = 0; i < cs.length(); i++) {
                for (int j = 0; j <= i; j++) {
                    assertEquals(i, CSUtils.skipUntil(cs, j, cs.charAt(i)),
                                "i="+i+",j="+j);
                }
            }
        }
    }

    @Test
    void testFindEarliest() {
        for (CharSequence cs : asList("aba", new StringBuilder("aba"))) {
            assertEquals(0, CSUtils.skipUntil(cs, 0, 'a'));
            assertEquals(2, CSUtils.skipUntil(cs, 1, 'a'));
        }
    }


    @ParameterizedTest @ValueSource(strings = {
            "0 | a | ",
            "0 | a | b",
            "1 | a | b",
            "0 | a | bb",
            "0 | a | bbc",
            "1 | a | bbc",
            "2 | a | bbc",
            "3 | a | bbc",
            "4 | a | bbc",
            "1 | a | a",
            "1 | a | ab",
            "2 | a | ab",
            "2 | b | abc",
            "3 | b | abc",
    })
    void testNegativeSkipUntil(String dataString) {
        String[] data = dataString.split(" *\\| *");
        int from = parseInt(data[0]);
        char ch = data[1].charAt(0);
        String string = data.length < 3 ? "" : data[2];
        for (CharSequence cs : asList(string, new StringBuilder(string)))
            assertEquals(string.length(), CSUtils.skipUntil(cs, from, ch));
    }

    public static Stream<Arguments> testSkipSpaceAnd() {
        return Stream.of(
                arguments("", 0, '-', 0),
                arguments("a", 0, '-', 0),
                arguments("\"", 0, '-', 0),
                arguments("~", 0, '-', 0),
                arguments("%20", 0, '-', 0),
                arguments("\na", 0, '-', 1),
                arguments("\na", 0, 'a', 2),
                arguments("\ra", 0, '-', 1),
                arguments("\ta", 0, '-', 1),
                arguments(" a", 0, '-', 1),
                arguments("-a", 0, '-', 1),
                arguments("a\ta", 1, '-', 2),
                arguments("a\na", 1, '-', 2),
                arguments("a\ra", 1, '-', 2),
                arguments("a-a", 1, '-', 2),
                arguments("a-\"", 1, '-', 2),
                arguments("\n-\"", 1, '-', 2)
        );
    }

    @ParameterizedTest @MethodSource
    void testSkipSpaceAnd(String cs, int from, char skip, int expected) {
        assertEquals(expected, CSUtils.skipSpaceAnd(cs, from, skip));
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
}