package com.github.alexishuf.fastersparql.client.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.util.Skip.alphabet;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SkipTest {
    @Test
    public void testMask() {
        for (int i = 0; i < 127; i++) {
            var msg = "i="+i;
            long[] alphabet = alphabet().add((char)i).get();
            long[] untilMask = alphabet(alphabet).invert().get();
            String in = "" + ((char) i);
            assertEquals(1, Skip.skip(in, 0, 1, alphabet), msg);
            assertEquals(0, Skip.skip(in, 0, 1, untilMask), msg);
            assertEquals(0, Skip.reverseSkip(in,0, 1, alphabet), msg);
        }
    }

    static Stream<Arguments> testSkip() {
        long[] letters = alphabet().letters().get();
        long[] digits = alphabet().digits().get();
        long[] varName = alphabet().letters().digits().add('_', '-', '.').get();
        return Stream.of(
                arguments("abc,def", letters, 3),
                arguments("Xa,cd", letters, 2),
                arguments("a,b", letters, 1),
                arguments(",b", letters, 0),
                arguments("0b", letters, 0),
                arguments("a^b", letters, 1),
                arguments("[a]", letters, 0),
                arguments("a]", letters, 1),
                arguments("", letters, 0),
                arguments("ç", letters, 0),
                arguments("aç", letters, 1),

                arguments("", digits, 0),
                arguments("1,2", digits, 1),
                arguments("9/2", digits, 1),
                arguments("5:2", digits, 1),
                arguments("5:2", digits, 1),
                arguments("ç5", digits, 0),
                arguments("5ã", digits, 1),

                arguments("", varName, 0),
                arguments("a0-._ ", varName, 5),
                arguments("a0-._;", varName, 5),
                arguments("a0-._$", varName, 5),
                arguments("a0-._?", varName, 5),
                arguments("A0-._[", varName, 5),
                arguments("Aç0-._[", varName, 1)
        );
    }

    @ParameterizedTest @MethodSource
    void testSkip(String in, long[] alphabet, int expected) {
        List<String> caps = List.of("", "a0,.+[@/:_>");
        for (String prefix : caps) {
            for (String suffix : caps) {
                String string = prefix + in + suffix;
                int begin = prefix.length(), end = begin+in.length();
                assertEquals(expected+begin, Skip.skip(string, begin, end, alphabet));
            }
        }
    }

    static Stream<Arguments> testSkipUntil() {
        long[] letters = alphabet().letters().get();
        long[] digits = alphabet().digits().get();
        long[] varName = alphabet().letters().digits().add('_', '-', '.').get();
        return Stream.of(
                arguments("0, \n", letters, 4),
                arguments("|/[a\n", letters, 3),
                arguments("", letters, 0),
                arguments("a", letters, 0),
                arguments("Z", letters, 0),
                arguments("z", letters, 0),
                arguments("A", letters, 0),
                arguments("ça", letters, 1),

                arguments("", digits, 0),
                arguments("0", digits, 0),
                arguments("9", digits, 0),
                arguments("aAzZ/[^|5", digits, 8),
                arguments("ç0", digits, 1),

                arguments("?a-.", varName, 1),
                arguments("?.0", varName, 1),
                arguments("?$.0", varName, 2),
                arguments("+$.0", varName, 2),
                arguments("~$.0", varName, 2)
        );
    }

    @ParameterizedTest @MethodSource
    void testSkipUntil(String in, long[] alphabet, int expected) {
        long[] untilMask = alphabet(alphabet).invert().get();
        List<String> caps = List.of("", "a0,.+[@/:_>");
        for (String prefix : caps) {
            for (String suffix : caps) {
                String string = prefix + in + suffix;
                int begin = prefix.length(), end = begin+in.length();
                assertEquals(expected+begin, Skip.skip(string, begin, end, untilMask));
            }
        }
    }

    static Stream<Arguments> testReverseSkip() {
        long[] ws = alphabet().whitespace().get();
        long[] dot = alphabet().whitespace().control().add('.').get();
        return Stream.of(
                arguments("nm (", ws, 3),
                arguments("nm ", ws, 1),
                arguments("?var. ", dot, 3),
                arguments("?var. \r\n", dot, 3),
                arguments("?va.r. \r\n", dot, 4),
                arguments("? va.r. \r\n", dot, 5)
        );
    }

    @ParameterizedTest @MethodSource
    void testReverseSkip(String in, long[] alphabet, int expected) {
        List<String> caps = List.of("", "a0,.+[@/:_>");
        for (String prefix : caps) {
            for (String suffix : caps) {
                String string = prefix + in + suffix;
                int begin = prefix.length(), end = begin+in.length();
                assertEquals(expected+begin, Skip.reverseSkip(string, begin, end, alphabet));
            }
        }
    }

}