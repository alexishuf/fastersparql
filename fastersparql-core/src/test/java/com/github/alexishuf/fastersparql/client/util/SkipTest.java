package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.Rope.alphabet;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SkipTest {
    private List<Rope> inputs(String string) {
        byte[] u8 = string.getBytes(UTF_8);
        return List.of(new ByteRope(string),
                       new ByteRope("."+string).sub(1, u8.length+1),
                       new SegmentRope(ByteBuffer.wrap(u8)));
    }

    @Test
    public void testMask() {
        for (int i = 0; i < 127; i++) {
            var msg = "i="+i;
            int[] alphabet = Rope.alphabet(String.valueOf((char) i));
            int[] untilMask = Rope.invert(alphabet);
            for (Rope in : inputs(String.valueOf((char) i))) {
                assertEquals(1, in.skip(0, 1, alphabet), msg);
                assertEquals(0, in.skip(0, 1, untilMask), msg);
                assertEquals(0, in.reverseSkip(0, 1, alphabet), msg);
            }
        }
    }

    static Stream<Arguments> testSkip() {
        int[] letters = Rope.LETTERS;
        int[] digits = Rope.DIGITS;
        int[] varName = alphabet("_-.", Rope.Range.ALPHANUMERIC);
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
    void testSkip(String midString, int[] alphabet, int expected) {
        List<String> caps = List.of("", "a0,.+[@/:_>");
        for (String prefix : caps) {
            for (String suffix : caps) {
                String string = prefix + midString + suffix;
                for (Rope r : inputs(string)) {
                    int begin = prefix.length(), end = r.len()-suffix.length();
                    assertEquals(expected+begin, r.skip(begin, end, alphabet));
                }
            }
        }
    }

    static Stream<Arguments> testSkipUntil() {
        int[] letters = Rope.LETTERS;
        int[] digits = Rope.DIGITS;
        int[] varName = alphabet("_-.", Rope.Range.ALPHANUMERIC);
        return Stream.of(
                arguments("0, \n", letters, 4),
                arguments("|/[a\n", letters, 3),
                arguments("", letters, 0),
                arguments("a", letters, 0),
                arguments("Z", letters, 0),
                arguments("z", letters, 0),
                arguments("A", letters, 0),
                arguments("ça", letters, 2),

                arguments("", digits, 0),
                arguments("0", digits, 0),
                arguments("9", digits, 0),
                arguments("aAzZ/[^|5", digits, 8),
                arguments("ç0", digits, 2),

                arguments("?a-.", varName, 1),
                arguments("?.0", varName, 1),
                arguments("?$.0", varName, 2),
                arguments("+$.0", varName, 2),
                arguments("~$.0", varName, 2)
        );
    }

    @ParameterizedTest @MethodSource
    void testSkipUntil(String mid, int[] alphabet, int expected) {
        int[] untilMask = Rope.invert(alphabet);
        List<String> caps = List.of("", "a0,.+[@/:_>");
        for (String prefix : caps) {
            for (String suffix : caps) {
                int begin = prefix.length(), end = begin+mid.getBytes(UTF_8).length;
                String ctx = "prefix=" + prefix + ", suffix=" + suffix + ", mid=" + mid;
                for (Rope r : inputs(prefix + mid + suffix)) {
                    assertEquals(expected+begin, r.skip(begin, end, untilMask), ctx);
                }
            }
        }
    }

    static Stream<Arguments> testReverseSkip() {
        int[] ws = Rope.WS;
        int[] dot = alphabet(".", Rope.Range.WS);
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
    void testReverseSkip(String mid, int[] alphabet, int expected) {
        List<String> caps = List.of("", "a0,.+[@/:_>");
        for (String prefix : caps) {
            for (String suffix : caps) {
                int begin = prefix.length(), end = begin+mid.getBytes(UTF_8).length;
                for (Rope r : inputs(prefix + mid + suffix))
                    assertEquals(expected+begin, r.reverseSkip(begin, end, alphabet));
            }
        }
    }

}