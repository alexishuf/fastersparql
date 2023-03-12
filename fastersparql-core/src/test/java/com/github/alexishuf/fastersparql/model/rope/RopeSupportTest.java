package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.RopeSupport.compareTo;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RopeSupportTest {

    public static Stream<Arguments> testRangeEquals() {
        byte[] a = "a".getBytes(UTF_8),   a1  = "a".getBytes(UTF_8);
        byte[] b = "b".getBytes(UTF_8);
        byte[] ab = "ab".getBytes(UTF_8), ab1 = "ab".getBytes(UTF_8);
        byte[] ac = "ac".getBytes(UTF_8);
        byte[] letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes(UTF_8);
        byte[] letters1 = Arrays.copyOf(letters, letters.length);
        List<Arguments> list = new ArrayList<>(List.of(
                arguments(true, a, 0, a1, 0, 1),
                arguments(false, a, 0, b, 0, 1),

                arguments(true, ab, 0, ab1, 0, 2),
                arguments(true, ab, 0, ab1, 0, 1),
                arguments(true, ab, 1, ab1, 1, 1),
                arguments(true, ab, 0, letters, 0, 2),

                arguments(false, ab, 0, ac, 0, 2),
                arguments(false, ab, 0, letters, 1, 2)
        ));
        for (int len : List.of(0, 1, 2, 8, 31, 32, 33, 44, letters.length-1)) {
            list.add(arguments(true, letters, 0, letters, 0, len));
            list.add(arguments(true, letters, 1, letters1, 1, len));
            list.add(arguments(len == 0, letters, 0, letters, 1, len));
            list.add(arguments(len == 0, letters, 1, letters1, 0, len));
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testRangeEquals(boolean expected, byte[] l, int off, byte[] r, int rOff, int len) {
        assertEquals(expected, RopeSupport.rangesEqual(l, off, r, rOff, len));
    }

    @ParameterizedTest @ValueSource(strings = {
            "@",
            "a@a",
            "aa@aa",
            "a@A",
            "01234567890123456789012345678901@01234567890123456789012345678901",
            "01234567890123456789012345678901@01234567890123456789012345678901a",
            "01234567890123456789012345678901a@01234567890123456789012345678901",
            "01234567890123456789012345678901a@01234567890123456789012345678901a",
            "01234567890123456789012345678901a@01234567890123456789012345678901b",
            "01234567890123456789012345678901abc@01234567890123456789012345678901abc",
            "01234567890123456789012345678901abc@01234567890123456789012345678901abx"
    })
    void testArraysEqual(String data) {
        String[] sides = data.split("@");
        String l = sides.length == 0 ? "" : sides[0], r = sides.length == 0 ? "" : sides[1];
        assertEquals(l.equals(r), RopeSupport.arraysEqual(l.getBytes(UTF_8), r.getBytes(UTF_8)));
    }

    static Stream<Arguments> testHash() {
        return Stream.of(
                arguments(""),
                arguments("a"),
                arguments("ab"),
                arguments("abc")
        );
    }

    @ParameterizedTest @MethodSource
    void testHash(String string) {
        byte[] u8 = string.getBytes(UTF_8);
        assertEquals(string.hashCode(), RopeSupport.hash(u8, 0, u8.length));

        byte[] padded = new byte[u8.length + 4];
        padded[0] = '<';
        padded[1] = 'x';
        padded[padded.length-2] = '7';
        padded[padded.length-1] = '>';
        arraycopy(u8, 0, padded, 2, u8.length);
        assertEquals(string.hashCode(), RopeSupport.hash(padded, 2, u8.length));
    }

    static Stream<Arguments> testSkip() {
        return Stream.of(
                arguments("0123", "0", 1),
                arguments("0123", "10", 2),
                arguments("0123", "10", 2),
                arguments("0123", "3", 0),
                arguments("0123", "a", 0),
                arguments("0123", "321a", 0),
                arguments("0123", "031a", 2),
                arguments("0123", "0123", 4),
                arguments("0123", "3210", 4)
        );
    }

    @ParameterizedTest @MethodSource
    void testSkip(String in, String alphabet, int expected) {
        byte[] u8 = in.getBytes(UTF_8);
        int[] set = Rope.alphabet(alphabet);
        assertEquals(expected, RopeSupport.skip(u8, 0, u8.length, set));

        String bogus = "abcd0123<>.: \n";
        byte[] padded = (bogus+in+bogus).getBytes(UTF_8);
        int begin = bogus.length(), end = bogus.length()+u8.length;
        expected += bogus.length();
        assertEquals(expected, RopeSupport.skip(padded, begin, end, set));
    }

    static Stream<Arguments> testReverseSkip() {
        return Stream.of(
                arguments("0123", "01", 3),
                arguments("0123", "3", 2),
                arguments("0123", "32", 1),
                arguments("0123", "43", 2),
                arguments("0123", "243", 1),
                arguments("0123", "a3210", 0)
        );
    }

    @ParameterizedTest @MethodSource
    void testReverseSkip(String in, String alphabet, int expected) {
        byte[] u8 = in.getBytes(UTF_8);
        int[] set = Rope.alphabet(alphabet);
        assertEquals(expected, RopeSupport.reverseSkip(u8, 0, u8.length, set));

        String bogus = "0123a.<>";
        byte[] padded = (bogus + in + bogus).getBytes(UTF_8);
        int begin = bogus.length(), end = bogus.length()+u8.length;
        expected += bogus.length();
        assertEquals(expected, RopeSupport.reverseSkip(padded, begin, end, set));
    }

    static Stream<Arguments> testCompareTo() {
        return Stream.of(
                arguments("", ""),
                arguments("a", "b"),
                arguments("ab", "ab"),
                arguments("ab", "ac"),
                arguments("a", "aa"),
                arguments("b", "ba"),
                arguments("b", "bc"),
                arguments("b", "bc"),
                arguments("0123456789012345678901234567890123", "0123456789"),
                arguments("0123456789012345678901234567890123", "0123456789012345678901234567890123"),
                arguments("0123456789012345678901234567890123", "01234567890123456789012345678901234"),
                arguments("0123456789012345678901234567890123", "0123456789012345678901234567890124")
        );
    }

    @ParameterizedTest @MethodSource
    void testCompareTo(String l, String r) {
        byte[] lU8 = l.getBytes(UTF_8), rU8 = r.getBytes(UTF_8);
        assertEquals(l.compareTo(r), compareTo(lU8, rU8));
        assertEquals(r.compareTo(l), compareTo(rU8, lU8));

        assertEquals(l.compareTo(r), compareTo(lU8, new ByteRope(r), 0, r.length()));
        assertEquals(l.compareTo(r), compareTo(lU8, new ByteRope(r+r), r.length(), 2*r.length()));
        assertEquals(l.compareTo(r), compareTo(lU8, new BufferRope(ByteBuffer.wrap(r.getBytes(UTF_8))), 0, r.length()));
        assertEquals(l.compareTo(r), compareTo(lU8, new BufferRope(ByteBuffer.wrap(("~"+r).getBytes(UTF_8))), 1, 1+r.length()));
        assertEquals(l.compareTo(r), compareTo(lU8, new BufferRope(ByteBuffer.wrap((" "+r).getBytes(UTF_8))), 1, 1+r.length()));
        assertEquals(l.compareTo(r), compareTo(lU8, Term.valueOf("\""+r+"\"@en"), 1, 1+r.length()));
    }
}