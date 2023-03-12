package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class ByteRopeTest {
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
        assertEquals(Long.toString(v), new ByteRope().append(v).toString());
        assertEquals("x"+v, new ByteRope().append('x').append(v).toString());
        for (String prefix : List.of("x", ".".repeat(63), "~".repeat(64)))
            assertEquals(prefix+v, new ByteRope().append(prefix).append(v).toString());
    }

    @Test
    void testCannotChangeEmpty() {
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.append('x'));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.append("123"));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.append(23));
        assertThrows(UnsupportedOperationException.class, EMPTY::clear);
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.fill('0'));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.append(Rope.of("rope")));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.append(Rope.of("rope"), 1, 2));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.append(new byte[] {'0', '1', '2'}, 1, 1));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.newline(0));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.newline(2));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.escapingLF("..\n++"));
        assertThrows(UnsupportedOperationException.class, () -> EMPTY.indented(2, "##"));
    }

    @Test void testIndented() {
        assertEquals("  ", new ByteRope().indented(2, "").toString());
        assertEquals("a    ", new ByteRope("a").indented(4, "").toString());
        assertEquals("  #", new ByteRope().indented(2, "#").toString());
        assertEquals("  #\n  $", new ByteRope().indented(2, "#\n$").toString());
        assertEquals("  #\n  $\n", new ByteRope().indented(2, "#\n$\n").toString());
        assertEquals("..  #\n  $\n", new ByteRope("..").indented(2, "#\n$\n").toString());
    }

    @Test void testEscapingLF() {
        assertEquals("1\\n2", new ByteRope().escapingLF("1\n2").toString());
        assertEquals("1\\n2", new ByteRope().escapingLF("1\\n2").toString());
        assertEquals(".1\\n2", new ByteRope(".").escapingLF("1\n2").toString());
    }

    @Test void testNewline() {
        assertEquals("\n ",  new ByteRope().newline(1).toString());
        assertEquals("\n  ", new ByteRope().newline(2).toString());
        assertEquals(".\n  ", new ByteRope(".").newline(2).toString());
    }

    @Test void testAppend() {
        ByteRope r = new ByteRope();
        assertEquals("0", r.append('0').toString());
        assertEquals("0@", r.append((byte)64).toString());
        assertEquals("0@-", r.append(Rope.of("-")).toString());
        assertEquals("0@-12", r.append(Rope.of("0123"), 1, 3).toString());
        assertEquals("", r.clear().toString());
    }

    @Test void testFill() {
        assertEquals("...", new ByteRope("123").fill('.').toString());
        assertEquals("", new ByteRope("").fill('.').toString());
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
            return data[pos++];
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
            var r = new ByteRope();
            List<String> actual = new ArrayList<>();
            while (r.clear().readLine(stream)) actual.add(r.toString());
            assertEquals(expected, actual);
        }
    }
}