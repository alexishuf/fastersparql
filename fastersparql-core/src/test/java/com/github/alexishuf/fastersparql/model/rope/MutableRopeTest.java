package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MutableRopeTest {
    public static Stream<Arguments> testAppendNumber() {
        List<Long> list = new ArrayList<>(List.of(1L, 0L, -1L));
        for (long i =  2; i <=  20; i++) list.add(i);
        for (long i = -2; i >= -20; i--) list.add(i);
        for (long i =  90; i <=  111; i++) list.add(i);
        for (long i = -90; i >= -111; i--) list.add(i);
        list.addAll(List.of(
                 947L,  999L,  1000L,  1001L,  1234L,
                -947L, -999L, -1000L, -1001L, -1234L
        ));
        list.addAll(List.of(
                (long)Integer.MIN_VALUE+1,  (long)Integer.MAX_VALUE-1,
                (long)Integer.MIN_VALUE,    (long)Integer.MAX_VALUE,
                (long)Integer.MIN_VALUE-1L, (long)Integer.MAX_VALUE+1L
        ));
        list.addAll(List.of(
                Long.MIN_VALUE+1, Long.MAX_VALUE-1,
                Long.MIN_VALUE,   Long.MAX_VALUE
        ));
        return list.stream().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testAppendNumber(long v) {
        try (var tmp = PooledMutableRope.get()) {
            assertEquals(Long.toString(v), tmp.clear().append(v).toString());
            assertEquals("x"+v, tmp.clear().append('x').append(v).toString());
            for (String prefix : List.of("x", ".".repeat(63), "~".repeat(64)))
                assertEquals(prefix+v, tmp.clear().append(prefix).append(v).toString());
        }
    }

    @Test void testIndented() {
        try (var tmp = PooledMutableRope.get()) {
            assertEquals("  ", tmp.clear().indented(2, "").toString());
            assertEquals("a    ", tmp.clear().append("a").indented(4, "").toString());
            assertEquals("  #", tmp.clear().indented(2, "#").toString());
            assertEquals("  #\n  $", tmp.clear().indented(2, "#\n$").toString());
            assertEquals("  #\n  $\n", tmp.clear().indented(2, "#\n$\n").toString());
            assertEquals("..  #\n  $\n", tmp.clear().append("..").indented(2, "#\n$\n").toString());
        }
    }

    @Test void testEscapingLF() {
        try (var tmp = PooledMutableRope.get()) {
            assertEquals("1\\n2", tmp.clear().appendEscapingLF("1\n2").toString());
            assertEquals("1\\n2", tmp.clear().appendEscapingLF("1\\n2").toString());
            assertEquals(".1\\n2", tmp.clear().append(".").appendEscapingLF("1\n2").toString());
        }
    }

    @Test void testNewline() {
        try (var tmp = PooledMutableRope.get()) {
            assertEquals("\n ",  tmp.clear().newline(1).toString());
            assertEquals("\n  ", tmp.clear().newline(2).toString());
            assertEquals(".\n  ", tmp.clear().append(".").newline(2).toString());
        }
    }

    @Test void testAppend() {
        try (var tmp = PooledMutableRope.get()) {
            assertEquals("0", tmp.append('0').toString());
            assertEquals("0@", tmp.append((byte)64).toString());
            assertEquals("0@-", tmp.append(Rope.asRope("-")).toString());
            assertEquals("0@-12", tmp.append(Rope.asRope("0123"), 1, 3).toString());
            assertEquals("", tmp.clear().toString());
        }
    }

    @Test void testFill() {
        try (var tmp = PooledMutableRope.get()) {
            assertEquals("...", tmp.clear().append("123").fill('.').toString());
            assertEquals("", tmp.clear().append("").fill('.').toString());
        }
    }


    private static List<String> readLineExpected(String input) {
        ArrayList<String> list = new ArrayList<>();
        try (var bis = new ByteArrayInputStream(input.getBytes(UTF_8));
             var reader = new BufferedReader(new InputStreamReader(bis, UTF_8))) {
            for (String line; (line = reader.readLine()) != null; )
                list.add(line);
        } catch (IOException e) {
            fail(e);
        }
        return list;
    }

    private static final class UnbufferedInputStream extends InputStream {
        private final byte[] data;
        private int pos;

        public UnbufferedInputStream(String data) {
            this.data = data.getBytes(UTF_8);
            this.pos = 0;
        }

        @Override public int read() {
            if (pos == data.length) return -1;
            return 0xff&data[pos++];
        }
    }

    static Stream<Arguments> testReadLine() {
        List<String> lfInputs = List.of(
                "",
                "one\n",
                "implicit",
                "\n",
                "1\n2\n",
                "1\n\n3\n",
                "1\n\n3\nimplicit",
                "1\n\t\n3\n\n"
        );
        return Stream.concat(
                lfInputs.stream(),
                lfInputs.stream().map(s -> s.replace("\n", "\r\n"))
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testReadLine(String input) throws IOException {
        var expected = readLineExpected(input);
        var streams = List.of(new UnbufferedInputStream(input),
                              new ByteArrayInputStream(input.getBytes(UTF_8)));
        for (InputStream stream : streams) {
            List<String> actual;
            try (var r = PooledMutableRope.get()) {
                actual = new ArrayList<>();
                while (r.clear().readLine(stream)) actual.add(r.toString());
            }
            assertEquals(expected, actual);
        }
    }

    static Stream<Arguments> testEncodeCharSubSequence() {
        return Stream.of(
                arguments("0123", 0, 4),
                arguments("0123", 1, 3),
                arguments("0ç2ã4", 0, 5),
                arguments("0ç2ã4", 1, 2),
                arguments("0ç2ã4", 1, 4),
                arguments(".".repeat(64)+"0ç2ã4", 64, 64+5),
                arguments("\uD83E\uDE02", 0, 2),
                arguments("maçã\uD83E\uDE02", 2, 6)
        );
    }

    @ParameterizedTest @MethodSource
    void testEncodeCharSubSequence(String string, int begin, int end) {
        try (var r = new MutableRope(4)) {
            r.append('@');
            assertSame(r, r.append(string, begin, end));
            r.append('#');
            assertEquals("@" + string.substring(begin, end) + "#", r.toString());
        }
    }

    @Test void testAppendCodePoint() {
        String string = "\t\u0000aA~\u007F" +
                "£\u0080ࣿ" +
                "€ह한" +
                "\uD800\uDF48";
        try (var tmp = PooledMutableRope.get()) {
            for (int codePoint : string.codePoints().toArray()) {
                String expected = "@" + Character.toString(codePoint) + ">";
                String actual = tmp.clear().append("@").appendCodePoint(codePoint)
                                           .append('>').toString();
                assertEquals(expected, actual);
            }
        }
    }
}