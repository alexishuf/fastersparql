package com.github.alexishuf.fastersparql.model.rope;

import com.github.alexishuf.fastersparql.sparql.expr.InvalidTermException;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.lang.Character.toUpperCase;
import static java.lang.Integer.signum;
import static java.lang.System.arraycopy;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RopeTest {
    private static final String FILLER = "abcdefghijklmnopqrstuvwxyz/~*-+.,{}[]??;.,<>_!@#$%*()ABCDEFGHIJKLMNOPRSTUVWXYZ";

    interface Factory {
        List<Rope> create(String string);

        default List<Rope> create(List<String> strings) {
            List<Rope> list = new ArrayList<>();
            for (String string : strings)
                list.addAll(create(string));
            return list;
        }
        default List<Rope> create(String... strings) { return create(Arrays.asList(strings)); }
    }

    private static final class ByteRopeFac implements Factory {
        @Override public List<Rope> create(String string) {
            return List.of(FinalSegmentRope.asFinal(string));
        }
        @Override public String toString() { return "ByteRopeFac"; }
    }

    private static final class FinalSegmentRopeSubRopeFac implements Factory {
        @Override public List<Rope> create(String string) {
            List<Rope> list = new ArrayList<>();
            byte[] utf8 = string.getBytes(UTF_8);
            list.add(new FinalSegmentRope(utf8));

            byte[] paddedShort = new byte[utf8.length + 2];
            paddedShort[0] = '<';
            paddedShort[paddedShort.length-1] = '\"';
            arraycopy(utf8, 0, paddedShort, 1, utf8.length);
            list.add(new FinalSegmentRope(paddedShort, 1, utf8.length));

            byte[] paddedLong = new byte[utf8.length + 64];
            byte[] junk = "@!.,{}()[]<>0+ -_:#/?%abcdefg^\"\0".getBytes(UTF_8);
            assertEquals(32, junk.length);
            arraycopy(junk, 0, paddedLong, 0, junk.length);
            arraycopy(utf8, 0, paddedLong, junk.length, utf8.length);
            arraycopy(junk, 0, paddedLong, junk.length+utf8.length, junk.length);
            list.add(new FinalSegmentRope(paddedLong, junk.length, utf8.length));
            return list;
        }
        @Override public String toString() { return "ByteSubRopeFac"; }
    }

    private static final class TwoSegmentRopeFac implements Factory {
        @Override public List<Rope> create(String string) {
            ArrayList<Rope> list = new ArrayList<>();
            byte[] u8 = string.getBytes(UTF_8);
            MemorySegment seg = MemorySegment.ofArray(u8);
            int n = (int) seg.byteSize();
            list.add(new TwoSegmentRope(seg, 0, n, seg, 0, 0));
            list.add(new TwoSegmentRope(seg, n, 0, seg, 0, n));

            var r = new TwoSegmentRope();
            r.wrapFirst(seg, u8, 0, n);
            list.add(r);

            r = new TwoSegmentRope();
            r.wrapSecond(seg, u8, 0, n);
            list.add(r);

            if (n > 2) {
                int mid = n>>1;
                list.add(new TwoSegmentRope(seg, 0, mid, seg, mid, n-mid));

                r = new TwoSegmentRope(seg, mid, n, seg, 0, n);
                r.wrapFirst(seg, u8, 0, mid);
                r.wrapSecond(seg, u8, mid, n-mid);
                list.add(r);

                r = new TwoSegmentRope();
                r.wrapSecond(seg, u8, 1, n-1);
                r.wrapFirst(seg, u8, 0, 1);
                list.add(r);

                r = new TwoSegmentRope(seg, mid, n-mid, seg, 0, mid);
                r.wrapSecond(seg, u8, 1, n-1);
                r.wrapFirst(seg, u8, 0, 1);
                list.add(r);
            }
            return list;
        }
        @Override public String toString() { return "TwoSegmentRopeFac"; }
    }

    private static final class TwoSegmentSubRopeFac implements Factory {
        @Override public List<Rope> create(String string) {
            var s = MemorySegment.ofArray(("." + string + ".").getBytes(UTF_8));
            int n = (int) s.byteSize();
            int mid = n>>1;
            var parent = new TwoSegmentRope(s, 0, mid, s, mid, n - mid);
            return List.of(parent.sub(1, n-1));
        }

        @Override public String toString() { return "TwoSegmentSubRopeFac"; }
    }

    private static final class BufferRopeFac implements Factory  {
        @Override public List<Rope> create(String string) {
            byte[] utf8 = string.getBytes(UTF_8);
            var tight = ByteBuffer.wrap(utf8).limit(utf8.length);
            var inner = ByteBuffer.allocate(utf8.length+10)
                                  .position(5).put(utf8)
                                  .position(5).limit(5+utf8.length);
            var innerDirect = ByteBuffer.allocateDirect(utf8.length+66)
                                        .position(33).put(utf8)
                                        .position(33).limit(33+utf8.length);
            return List.of(new FinalSegmentRope(tight), new FinalSegmentRope(inner),
                           new FinalSegmentRope(innerDirect));
        }
        @Override public String toString() { return "BufferRopeFac"; }
    }

    private static final class TermRopeFac implements Factory {
        @Override public List<Rope> create(String string) {
            List<Rope> list = new ArrayList<>();
            try {
                Term term = Term.valueOf(string);
                if (term == null)
                    return List.of();
                list.add(term);
            } catch (InvalidTermException|AssertionError e) {
                return List.of();
            }
            byte[] u8 = string.getBytes(UTF_8);
            list.add(Term.valueOf(new FinalSegmentRope(u8)));
            list.add(Term.valueOf(new FinalSegmentRope(ByteBuffer.wrap(u8))));
            try (var tmp = PooledMutableRope.getWithCapacity(u8.length)) {
                list.add(Term.valueOf(tmp.append(u8)));
            } // invalidate tmp content, Term.valueOf() must have copied the bytes
            if (string.startsWith("\"") && string.matches("\"(@[a-zA-Z\\-]+)?$")) {
                list.add(Term.wrap(new FinalSegmentRope(u8), null));
                list.add(Term.wrap(null, new FinalSegmentRope(u8)));
                list.add(Term.wrap(new FinalSegmentRope(u8), FinalSegmentRope.EMPTY));
                list.add(Term.wrap(FinalSegmentRope.EMPTY, new FinalSegmentRope(u8)));
            }

            try (var tmp = PooledMutableRope.getWithCapacity(string.length()+2)) {
                tmp.append('.').append(string).append('.');
                list.add(Term.valueOf(tmp, 1, tmp.len()-1));
            }
            assertTrue(list.stream().noneMatch(Objects::isNull), "null ropes generated");
            return list;
        }
        @Override public String toString() { return "TermRopeFac"; }
    }

    private static final TwoSegmentSubRopeFac TWO_SEGMENT_ROPE_FAC = new TwoSegmentSubRopeFac();
    private static final List<Factory> FACTORIES = List.of(
            new ByteRopeFac(),
            new FinalSegmentRopeSubRopeFac(),
            new BufferRopeFac(),
            new TwoSegmentRopeFac(),
            TWO_SEGMENT_ROPE_FAC,
            new TermRopeFac()
    );

    private static int fnv(StringBuilder sb) {
        int h = Rope.FNV_BASIS;
        for (int i = 0; i < sb.length(); i++)
            h = Rope.FNV_PRIME * (h ^ (0xff&sb.charAt(i)));
        return h;
    }

    static Stream<Arguments> factories() { return FACTORIES.stream().map(Arguments::arguments); }

    @SuppressWarnings("SimplifiableAssertion") @ParameterizedTest @MethodSource("factories")
    void testRead(Factory fac) {
        for (int len = 0; len < 128; len++) {
            var sb = new StringBuilder();
            for (int i = 0; i < len; i++)
                sb.append((char)(32 + (i % (127-32))));
            for (Rope rope : fac.create(sb.toString())) {
                assertEquals(len, rope.len());
                assertEquals(len, rope.length());
                for (int i = 0; i < len; i++) {
                    assertEquals(sb.charAt(i), rope.get(i), "i="+i);
                    assertEquals(sb.charAt(i), rope.charAt(i), "i="+i);
                    assertArrayEquals(new byte[] {(byte)(0xff&sb.charAt(i))},
                                      rope.toArray(i, i+1));
                    assertEquals(0, rope.toArray(i, i).length);
                    byte[] array = new byte[2];
                    array[0] = (byte)0xff;
                    rope.copy(i, i+1, array, 1);
                    assertArrayEquals(new byte[] {(byte)0xff, (byte)(0xff&sb.charAt(i))}, array);
                }
                assertThrows(IndexOutOfBoundsException.class, () -> rope.get(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> rope.get(-2));
                assertThrows(IndexOutOfBoundsException.class, () -> rope.get(rope.len()));
                assertThrows(IndexOutOfBoundsException.class, () -> rope.get(rope.len()+1));
                assertThrows(IndexOutOfBoundsException.class, () -> rope.toArray(-1, rope.len()));
                assertThrows(IndexOutOfBoundsException.class, () -> rope.toArray(rope.len()-1, rope.len()+1));

                assertEquals(sb.toString(), rope.toString());
                assertEquals(0, CharSequence.compare(sb, rope));
                assertEquals(sb.substring(0, sb.length()), rope.sub(0, rope.len()).toString());
                for (int i = 0; i < len; i++)
                    assertEquals(sb.substring(i, i+1), rope.sub(i, i+1).toString());

                assertTrue(rope.has(0, rope.toArray(0, len)));
                assertTrue(rope.hasAnyCase(0, rope.toArray(0, len)));
                for (int i = 0; i < len; i++) {
                    assertTrue(rope.has(i, new byte[0]));
                    assertTrue(rope.hasAnyCase(i, new byte[0]));
                }

                var lhs = FinalSegmentRope.asFinal(rope, 0, len);
                assertTrue(lhs.equals(rope));
                assertTrue(rope.equals(lhs));
                assertEquals(fnv(sb), rope.hashCode());
                assertEquals(lhs.toString(), rope.toString());
                assertEquals(sb.toString(), rope.toString());

                for (int i = 0; i < len; i++) {
                    assertEquals(i, rope.skipUntil(i, len, sb.charAt(i)));
                    assertEquals(i, rope.skipUntil(i, len, sb.charAt(i), '\n'));
                    assertEquals(i, rope.skipUntil(i, len, new byte[]{(byte)(0xff&sb.charAt(i))}));
                    int[] until = Rope.invert(Rope.alphabet(sb.substring(i, i + 1)));
                    assertEquals(i, rope.skip(i, len, until));
                    assertEquals(sb.charAt(i), rope.skipAndGet(i, len, until));
                }
            }
        }
    }

    static Stream<Arguments> testTerms() {
        return Stream.of(
                arguments("\"\"@en"),
                arguments("\"\""),

                arguments("\"a\"@en"),
                arguments("\"a\""),
                arguments("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>"),

                arguments("\"ab\"@en"),
                arguments("\"ab\""),
                arguments("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>"),

                arguments("\"\"@en-US"),
                arguments("\"1\"@en-US"),
                arguments("\".3\"@en-US"),

                arguments("<http://example.org/Alice>"),
                arguments("<http://example.org/ns#Bob>"),
                arguments("<http://example.org/users/Charlie>"),
                arguments("<http://example.org/users/search.php?name=Dave>"),

                arguments("\"2.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
                arguments("\"-2.3\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
                arguments("\"-2.3e+02\"^^<http://www.w3.org/2001/XMLSchema#double>")
        );
    }

    @SuppressWarnings({"SimplifiableAssertion", "EqualsWithItself"})
    @ParameterizedTest @MethodSource
    void testTerms(String string) {
        List<Rope> ropes = new TermRopeFac().create(string);
        for (Rope rope : ropes) {
            assertEquals(string.length(), rope.len());
            assertEquals(string, rope.toString());
            for (int i = 0; i < string.length(); i++) {
                assertEquals(string.charAt(i), rope.get(i));
                assertEquals(string.charAt(i), rope.sub(i, i+1).get(0));

                assertEquals(string.substring(i, i+1), rope.sub(i, i+1).toString());
                byte bVal = (byte)(0xff & string.charAt(i));
                assertTrue(rope.has(i, new byte[]{bVal}));
                assertTrue(rope.hasAnyCase(i, new byte[]{bVal}));
                assertTrue(rope.hasAnyCase(i, new byte[]{(byte)toUpperCase(string.charAt(i))}));
                assertEquals(i, rope.skipUntil(i, string.length(), string.charAt(i)));
                assertEquals(i, rope.skipUntil(i, string.length(), string.charAt(i), string.charAt(i)));
                assertEquals(i, rope.skipUntil(i, string.length(), new byte[]{bVal}));

                int[] until = Rope.invert(Rope.alphabet(string.substring(i, i + 1)));
                assertEquals(i, rope.skip(i, string.length(), until));
                assertEquals(string.charAt(i), rope.skipAndGet(i, string.length(), until));
            }
            assertThrows(IndexOutOfBoundsException.class, () -> rope.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> rope.get(-2));
            assertThrows(IndexOutOfBoundsException.class, () -> rope.get(string.length()));
            assertThrows(IndexOutOfBoundsException.class, () -> rope.get(string.length()+1));
            assertEquals(string, new String(rope.toArray(0, string.length()), UTF_8));
            byte[] a = new byte[string.length()+2];
            a[0] = '!';
            a[a.length-1] = '\"';
            rope.copy(0, string.length(), a, 1);
            assertEquals("!"+string+"\"", new String(a, UTF_8));
            assertEquals(string, rope.toString());
            assertTrue(rope.equals(rope));
        }
        for (Rope rope : ropes) {
            assertTrue(rope.equals(ropes.getFirst()));
            assertTrue(ropes.getFirst().equals(rope));
            assertEquals(ropes.getFirst().hashCode(), rope.hashCode());
        }
    }

    private static List<Rope> iriWrap(Factory fac, String prefix, String iri, String suffix) {
        return fac.create(iri.replaceAll("^<", "<"+prefix).replaceAll(">$", suffix+">"));
    }

    @ParameterizedTest @MethodSource("factories")
    void testSkipUntilLast(Factory fac) {
        int FL = FILLER.length();
        for (String bfr : List.of("", "#")) {
            for (String aft : List.of("", "#/")) {
                int off = bfr.length();
                int aftRegress = aft.isEmpty() ? 0 : aft.length() + 1;
                for (Rope r : iriWrap(fac, bfr, "<http://dbpedia.org/resource/Bob>", aft)) {
                    int end = r.len()-aftRegress;
                    int expected = off+28;
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/', (byte)'x'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'x', (byte)'/'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/', (byte)'#'));
                    assertEquals(expected, r.skipUntilLast(off, end, "/".getBytes(UTF_8)));
                    assertEquals(expected, r.skipUntilLast(off, end, "/Bob".getBytes(UTF_8)));
                    assertEquals(end, r.skipUntilLast(off, end, "/Alice".getBytes(UTF_8)));
                }
                for (Rope r : iriWrap(fac, bfr, "<http://dbpedia"+FILLER+".org/resource/Bob>", aft)) {
                    int end = r.len()-aftRegress;
                    int expected = off + FL + 28;
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/', (byte)'a'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/', (byte)'#'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'#', (byte)'/'));
                    assertEquals(expected, r.skipUntilLast(off, end, "/".getBytes(UTF_8)));
                    assertEquals(expected, r.skipUntilLast(off, end, "/Bob".getBytes(UTF_8)));
                    assertEquals(end, r.skipUntilLast(off, end, "/Alice".getBytes(UTF_8)));
                }
                for (Rope r : iriWrap(fac, bfr, "<http://example.org/ns#Alice>", aft)) {
                    int end = r.len()-aftRegress;
                    int expected = off + 22;
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'#'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'#', (byte)'o'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/', (byte)'#'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'#', (byte)'/'));
                    assertEquals(expected, r.skipUntilLast(off, end, "#".getBytes(UTF_8)));
                    assertEquals(expected, r.skipUntilLast(off, end, "#Alice".getBytes(UTF_8)));
                    assertEquals(expected, r.skipUntilLast(off, end, "#Alice".getBytes(UTF_8)));
                    assertEquals(0, r.skipUntilLast(0, end, ("<"+bfr+"http://").getBytes(UTF_8)));
                    assertEquals(off+1, r.skipUntilLast(off, end, "http://".getBytes(UTF_8)));
                    assertEquals(end, r.skipUntilLast(off, end, "/Alice".getBytes(UTF_8)));
                    assertEquals(end, r.skipUntilLast(off, end, "/Alicx".getBytes(UTF_8)));
                    assertEquals(end, r.skipUntilLast(off, end, "/Alicia".getBytes(UTF_8)));
                }
                for (Rope r : iriWrap(fac, bfr, "<http://example.org/"+FILLER+"ns#Alice>", aft)) {
                    int end = r.len()-aftRegress;
                    int expected = off + FL + 22;
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'#'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'#', (byte)'o'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'/', (byte)'#'));
                    assertEquals(expected, r.skipUntilLast(off, end, (byte)'#', (byte)'/'));
                    assertEquals(expected, r.skipUntilLast(off, end, "#".getBytes(UTF_8)));
                    assertEquals(expected, r.skipUntilLast(off, end, "#Alice".getBytes(UTF_8)));
                    assertEquals(expected, r.skipUntilLast(off, end, "#Alice".getBytes(UTF_8)));
                    assertEquals(0, r.skipUntilLast(0, end, ("<"+bfr+"http://").getBytes(UTF_8)));
                    assertEquals(off+1, r.skipUntilLast(off, end, "http://".getBytes(UTF_8)));
                    assertEquals(end, r.skipUntilLast(off, end, "#Alicx".getBytes(UTF_8)));
                    assertEquals(end, r.skipUntilLast(off, end, "#Alicia".getBytes(UTF_8)));
                }
            }
        }
        for (Rope r : fac.create("<http://www.example.org/ns#abcdefghijklmnopqrstuvxwyzABCDE000>")) {
            assertEquals(26, r.skipUntilLast(0, r.len(), (byte)'/', (byte)'#'));
        }
    }



    @ParameterizedTest @MethodSource("factories")
    void testSkip(Factory fac) {
        var sb = new StringBuilder().append('"');
        for (int i = 0; i < 128; i++)
            sb.append((char)i);
        sb.append('"');

        List<Rope> ropes = fac.create(sb.toString());
        for (Rope rope : ropes) {
            for (int i = 0; i < 127; i++) {
                int[] alphabet = Rope.alphabet(String.valueOf((char) i));
                int[] until = Rope.invert(Rope.alphabet(String.valueOf((char) i)));
                int end = sb.length() - 1, idx = i + 1;
                assertEquals(idx + 1, rope.skip(idx, end, alphabet));
                assertEquals(idx, rope.skip(1, end, until));
                assertEquals(idx, rope.skip(idx, end, until));
                if (i > 1)
                    assertEquals(idx, rope.skip(idx - 1, end, until));
                assertEquals(end, rope.skip(idx + 1, end, until));
                assertEquals(idx - 1, rope.sub(1, idx).skip(0, idx - 1, until));
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testSkipRanges(Factory fac) {
        int[] ws = Rope.alphabet(",", Rope.Range.WS);

        for (Rope rope : fac.create("a\t\t\n,.")) {
            assertEquals(0, rope.skip(0, rope.len(), ws));
            assertEquals(2, rope.skip(1, 2, ws));
            assertEquals(5, rope.skip(1, rope.len(), ws));
        }

        int[] dg = Rope.alphabet(".+-eE", Rope.Range.DIGIT);
        for (Rope rope : fac.create("a0123456789.+-eE09,")) {
            assertEquals(0, rope.skip(0, rope.len(), dg));
            assertEquals(2, rope.skip(1, 2, dg));
            assertEquals(12, rope.skip(1, 12, dg));
            assertEquals(18, rope.skip(1, rope.len(), dg));
        }

        int[] lt = Rope.alphabet("_", Rope.Range.LETTER);
        for (Rope rope : fac.create("<abc_ABC )")) {
            assertEquals(0, rope.skip(0, rope.len(), lt));
            assertEquals(5, rope.skip(1, 5, lt));
            assertEquals(8, rope.skip(1, rope.len(), lt));
        }

        int[] an = Rope.alphabet(".", Rope.Range.ALPHANUMERIC);
        for (Rope rope : fac.create("(09az.AZ950_")) {
            assertEquals(0, rope.skip(0, rope.len(), an));
            assertEquals(5, rope.skip(1, 5, an));
            assertEquals(6, rope.skip(1, 6, an));
            assertEquals(11, rope.skip(1, rope.len(), an));
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testToString(Factory fac) {
        List<String> strings = List.of("", "a", "<rel>", "\"23\"@en", "\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                "<http://www.w3.org/2001/XMLSchema#integer>");
        for (String str : strings) {
            for (Rope r : fac.create(str)) {
                assertEquals(str, r.toString());
                assertEquals(str, r.toString(0, r.len));
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testSkipNonAscii(Factory fac) {
        int[] alphabet = Rope.withNonAscii(Rope.alphabet("", Rope.Range.ALPHANUMERIC));
        int[] until = Rope.invert(alphabet);
        for (Rope rope : fac.create("ação\uD83E\uDE02X2.")) {
            assertEquals(13, rope.len());
            for (int i = 0; i < 12; i++)
                assertEquals(i, rope.skip(0, i, alphabet));
            assertEquals(12, rope.skip(0, rope.len(), alphabet));
            for (int i = 0; i < 12; i++)
                assertEquals(i, rope.skip(i, rope.len(), until));
        }
        for (Rope rope : fac.create(" \t\n\",\uD83E\uDE02\"X")) {
            assertEquals(11, rope.len());
            assertEquals(5, rope.skip(0, rope.len(), until));
            for (int i = 0; i < 5; i++)
                assertEquals(i, rope.skip(0, i, until));
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testParseLong(Factory fac) {
        record D(String in, Long ex) {}
        List<D> data = List.of(
                new D("", null),
                new D(" ", null),
                new D(".", null),
                new D("one", null),
                new D(" 1", null),

                new D("1", 1L),
                new D("9", 9L),
                new D("0", 0L),

                new D("1:", 1L),
                new D("9:", 9L),
                new D("0:", 0L),

                new D("+1", 1L),
                new D("+7", 7L),
                new D("+0", 0L),

                new D("-1", -1L),
                new D("-3", -3L),
                new D("-0", 0L),

                new D("23", 23L),
                new D("-23", -23L),

                new D("247", 247L),
                new D("-247", -247L),

                new D("987654321", 987654321L),
                new D("-987654321", -987654321L),

                new D("908706543021", 908706543021L),
                new D("-908706543021", -908706543021L)
        );
        for (var d : data) {
            var errCls = d.in.isEmpty() ? IndexOutOfBoundsException.class
                                        : NumberFormatException.class;
            for (Rope rope : fac.create(d.in)) {
                if (d.ex != null)
                    assertEquals(rope.parseLong(0), d.ex);
                else
                    assertThrows(errCls, () -> rope.parseLong(0));
            }
            for (Rope rope : fac.create(" "+d.in)) {
                if (d.ex != null)
                    assertEquals(rope.parseLong(1), d.ex);
                else
                    assertThrows(errCls, () -> rope.parseLong(1));
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testConvertCase(Factory fac) {
        var strings = List.of("", "a", "A", "0a", "\"\"", "\"a\"", "\"a\"", "\"a0bCd\"@en");
        for (String string : strings) {
            for (Rope r : fac.create(string)) {
                assertEquals(string.toUpperCase(), r.toAsciiUpperCase().toString());
                assertEquals(string.toLowerCase(), r.toAsciiLowerCase().toString());
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testTrim(Factory fac) {
        for (String string : List.of("", " ", "\t \ra\n", "\"\t\\r\\n \"", "\" \"@en")) {
            for (Rope r : fac.create(string)) {
                String trimmed = string.trim();
                assertEquals(trimmed, r.trim().toString());
                assertEquals(string.indexOf(trimmed) + trimmed.length(), r.rightTrim(0, r.len()));
            }
        }
    }


    @ParameterizedTest @MethodSource("factories")
    void testSkipWS(Factory fac) {
        for (String string : List.of("", " ", "\n ", " .")) {
            int expected = string.length() - string.trim().length();
            for (Rope r : fac.create(string)) {
                assertEquals(expected, r.skipWS(0, r.len()));
                assertEquals(expected, r.skip(0, r.len(), Rope.WS));
            }
            for (Rope r : fac.create("!"+string)) {
                assertEquals(expected + 1, r.skipWS(1, r.len()));
                assertEquals(expected + 1, r.skip(1, r.len(), Rope.WS));
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testIsAscii(Factory fac) {
        var ascii = List.of("", " ", "a", "23\t", "~\0\n}", "\"\"", "\" \"", "<a>", "\"~\0}\"");
        var unicode = List.of("ç", "não", "🨀");
        for (Rope r : fac.create(ascii))
            assertTrue(r.isAscii());
        for (Rope r : fac.create(unicode))
            assertFalse(r.isAscii());
    }

    @ParameterizedTest @MethodSource("factories")
    void testSkipUntil(Factory fac) {
        for (Rope r1 : fac.create("\"a.\"", "\"ç.\"", "\"\uD83E\uDE00.\"", "\"\0\r\n\t .\"")) {
            for (int begin = 0; begin <= r1.len()-2; begin++) {
                assertEquals(r1.len()-2, r1.skipUntil(begin, r1.len(), '.'));
                for (Rope r2 : fac.create(r1.sub(0, r1.len() - 2) + "!\"")) {
                    assertEquals(r1.len()-2, r1.skipUntil(begin, r1.len(), '.', '!'));
                    assertEquals(r2.len()-2, r2.skipUntil(begin, r2.len(), '.', '!'));
                }
            }
        }
        for (Rope r : fac.create("\"a>b->\"", "\"a-b->\"", "\"-\n>->.->\"", "\".>0->-\"", "\".>0->->\"")) {
            byte[] seq = {'-', '>'};
            for (int begin = 0; begin <= 4; begin++)
                assertEquals(4, r.skipUntil(begin, r.len(), seq));
            for (Rope r2 : fac.create("\"->" + r.sub(1, r.len()))) {
                for (int begin = 2; begin <= 6; begin++)
                    assertEquals(6, r2.skipUntil(begin, r2.len(), seq));
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testSkipUntilUnescaped(Factory fac) {
        for (Rope r : fac.create("", "0", "01", "012")) {
            for (int i = 0; i < r.len(); i++) {
                assertEquals(i, r.skipUntilUnescaped(0, r.len(), (char)r.get(i)));
                assertEquals(i, r.skipUntilUnescaped(0, r.len(), new byte[]{r.get(i)}));
            }
        }

        var single = List.of(
                "\"a\\nb\"",
                "\"\\r\"",
                "\"'\\\\'\"",
                "\"a\\\"b\"",
                "\"\\\"\"");
        var singleSuffixed = single.stream().map(s -> s + "\"").toList();
        var triple = List.of(
                "''''''",
                "'''a'''",
                "'''a\\'b'''",
                "'''\\''''",
                "'''a\\'''b'''",
                "'''\"'''",
                "'''\"'b'''",
                "'''\\\"'b'''",
                "'''\\r''b'''"
        );
        var tripleSuffixed = triple.stream().map(s -> s + "'''").toList();
        byte[] simpleQuote = {'"'};
        byte[] tripleQuote = {'\'', '\'', '\''};

        for (Rope r : fac.create(single)) {
            assertEquals(r.len()-1, r.skipUntilUnescaped(1, r.len(), '"'));
            assertEquals(r.len()-1, r.skipUntilUnescaped(1, r.len(), simpleQuote));
        }
        for (Rope r : fac.create(singleSuffixed)) {
            assertEquals(r.len()-2, r.skipUntilUnescaped(1, r.len(), '"'));
            assertEquals(r.len()-2, r.skipUntilUnescaped(1, r.len(), simpleQuote));
        }
        for (Rope r : fac.create(triple))
            assertEquals(r.len()-3, r.skipUntilUnescaped(3, r.len(), tripleQuote));
        for (Rope r : fac.create(tripleSuffixed))
            assertEquals(r.len()-6, r.skipUntilUnescaped(3, r.len(), tripleQuote));
    }

    @ParameterizedTest @MethodSource("factories")
    void testCompareTo(Factory fac) {
        record D(String left, String right) {
            @SuppressWarnings("EqualsWithItself") void test(Factory f, int row) {
                String ctx = "at data["+row+"]="+D.this;
                int expected = left.compareTo(right);
                for (Rope lr : f.create(left)) {
                    assertEquals(0, lr.compareTo(lr));
                    for (Rope rr : f.create(right)) {
                        assertEquals(0, rr.compareTo(rr));
                        assertEquals(signum( expected), signum(lr.compareTo(rr)), ctx);
                        assertEquals(signum(-expected), signum(rr.compareTo(lr)), ctx);
                        assertEquals(expected == 0, lr.equals(rr));
                    }
                    for (String rString : List.of(" " + right + " ", "~" + right + "~")) {
                        for (Rope rr : f.create(rString)) {
                            assertEquals(0, rr.compareTo(rr));
                            assertEquals(signum(expected), signum(lr.compareTo(rr, 1, rr.len - 1)), ctx);
                            assertEquals(expected == 0, lr.equals(rr.sub(1, rr.len-1)));
                        }
                    }
                    if (f instanceof ByteRopeFac || f instanceof BufferRopeFac) {
                        for (Rope rr : TWO_SEGMENT_ROPE_FAC.create(right)) {
                            assertEquals( signum(expected), signum(lr.compareTo(rr)));
                            assertEquals(-signum(expected), signum(rr.compareTo(lr)));

                            assertEquals( signum(expected), signum(lr.compareTo(rr, 0, rr.len)));
                            assertEquals(-signum(expected), signum(rr.compareTo(lr, 0, lr.len)));
                            assertEquals(expected == 0, lr.equals(rr));
                        }
                    }
                }
            }
        }
        List<D> data = new ArrayList<>(List.of(
                new D("1", "1"),
                new D("1", "2"),
                new D("11", "22"),
                new D("11", "12"),
                new D("11", "20"),
                new D("a", "a"),
                new D("a", "A"),
                new D("acb,.*&%#@123", "acb,.*&%#@123"),
                new D("acb,.*&%#@123", "acb,.*&%#@12~"),
                new D("", "1"),
                new D("1", "12"),
                new D("3", "32"),
                new D("3", "3...."),
                new D("200", "3"),
                new D("299", "300"),
                new D("299", "300")
        ));
        String pad32 = "_".repeat(32);
        String pad48 = pad32+"-".repeat(16);
        for (int i = 0, n = data.size(); i < n; i++) {
            D d = data.get(i);
            data.add(new D(pad32+d.left, pad32+d.right));
            data.add(new D(pad48+d.left, pad48+d.right));
        }
        for (int row = 0; row < data.size(); row++)
            data.get(row).test(fac, row);
    }

    @ParameterizedTest @MethodSource("factories")
    void testFastHash(Factory fac) {
        var sb = new StringBuilder();
        for (int i = 0; i < 32; i++) {
            sb.setLength(0);
            for (int j = 0; j < i; j++) sb.append((char)('0'+j));
            int expected = Rope.FNV_BASIS;
            for (int j = 0; j < 4 && j < i; j++)
                expected = Rope.FNV_PRIME * (expected ^ (0xff&sb.charAt(j)));
            for (int j = Math.max(i-12, 4); j < i; j++)
                expected = Rope.FNV_PRIME * (expected ^ (0xff&sb.charAt(j)));

            for (Rope r : fac.create(sb.toString()))
                assertEquals(expected, r.fastHash(0, r.len));
            for (Rope r : fac.create(sb + ")"))
                assertEquals(expected, r.fastHash(0, r.len-1));
            for (Rope r : fac.create("(" + sb))
                assertEquals(expected, r.fastHash(1, r.len));
            for (Rope r : fac.create("(" + sb + ")"))
                assertEquals(expected, r.fastHash(1, r.len-1));
            for (Rope r : fac.create("{([<:" + sb + ":>])}"))
                assertEquals(expected, r.fastHash(5, r.len-5));
        }
    }
    @ParameterizedTest @MethodSource("factories")
    void testReverseSkipUntil(Factory fac) {
        record D(String in, int ex) {
            void test(Factory f, int row) {
                var ctx = "at data[" + row + "]=" + this;
                for (Rope r : f.create(in))
                    assertEquals(ex, r.reverseSkipUntil(0, r.len(), '/'), ctx);
                for (Rope r : f.create('/'+in+'/'))
                    assertEquals(ex +1, r.reverseSkipUntil(1, r.len()-1, '/'), ctx);
            }
        }
        List<D> data = List.of(
                new D("", 0),
                new D("/", 0),
                new D("a", 0),
                new D("a/", 1),
                new D("/a", 0),
                new D("//", 1),
                new D("<http://ex.org/1>", 14),
                new D("<http://ex.org/1/2#>", 16),
                new D("<http://example.org/filllllller/one/two/three#>", 39)
        );
        for (int i = 0; i < data.size(); i++)
            data.get(i).test(fac, i);
    }

    @Test void testErase() {
        try (var tmp = PooledMutableRope.get()) {
            assertEquals("",    tmp.clear().append("")    .erase(0, 0).toString());
            assertEquals("",    tmp.clear().append("")    .erase(0, 1).toString());
            assertEquals("",    tmp.clear().append("0123").erase(0, 4).toString());
            assertEquals("0",   tmp.clear().append("0123").erase(1, 4).toString());
            assertEquals("01",  tmp.clear().append("0123").erase(2, 4).toString());
            assertEquals("013", tmp.clear().append("0123").erase(2, 3).toString());
            assertEquals("03",  tmp.clear().append("0123").erase(1, 3).toString());
            assertEquals("3",   tmp.clear().append("0123").erase(0, 3).toString());
            assertEquals("23",  tmp.clear().append("0123").erase(0, 2).toString());
        }
    }

    @Test void testReplace() {
        try (var tmp = PooledMutableRope.get()) {
            //empty string
            assertEquals("", tmp.clear().append("").replace('0', 'x').toString());

            //replace on short strings (no vectorization)
            assertEquals("x", tmp.clear().append("0").replace('0', 'x').toString());
            assertEquals("0x2", tmp.clear().append("012").replace('1', 'x').toString());
            assertEquals("x12", tmp.clear().append("012").replace('0', 'x').toString());
            assertEquals("01x", tmp.clear().append("012").replace('2', 'x').toString());
            assertEquals("x1x", tmp.clear().append("010").replace('0', 'x').toString());
            assertEquals("0x0", tmp.clear().append("010").replace('1', 'x').toString());


            var strings = List.of("012345678901234567890123456789012",
                    "01234567890123456789012345678900123456789012345678901234567890123");
            for (String string : strings) {
                for (char digit = '0'; digit <= '9'; digit++) {
                    var ex = string.replace(digit, 'x');
                    var ctx = "digit=" + digit + ", string=" + string;
                    assertEquals(ex, tmp.clear().append(string).replace(digit, 'x').toString(), ctx);
                }
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testSkipUntilLineBreak(Factory fac) {
        for (Rope r : fac.create(""))
            assertEquals(0, r.skipUntilLineBreak(0, 0));
        for (Rope r : fac.create("\"\\n\""))
            assertEquals(r.len(), r.skipUntilLineBreak(0, r.len()));
        for (String prefix : List.of("", "a", " ", "\t\ra ", "\r")) {
            for (String suffix : List.of("", "a", "\r", "\r\n", "\n")) {
                for (String lb : List.of("\n", "\r\n")) {
                    if (prefix.endsWith("\r") && lb.startsWith("\n")) continue;
                    String ctx = ("prefix="+prefix+", suffix="+suffix+", lb="+lb)
                            .replace("\r", "\\r").replace("\n", "\\n");
                    for (Rope r : fac.create(prefix+lb+suffix)) {
                        long val = r.skipUntilLineBreak(0, r.len());
                        assertEquals(prefix.length(), (int) val, ctx);
                        assertEquals(lb.length(), (int) (val >>> 32), ctx);
                    }
                }
            }
        }
    }

    @ParameterizedTest @MethodSource("factories")
    void testWrite(Factory fac) throws IOException {
        List<String> strings = List.of("\"alice\"",
                "\"bob\"@en-US",
                "\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                "\"23.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
                "<rel>",
                "<>",
                "<http://example.org/Alice>",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
        for (String string : strings) {
            for (Rope r : fac.create(string)) {
                var os = new ByteArrayOutputStream();
                r.write(os);
                assertEquals(r.toString(), os.toString(UTF_8));
            }
        }
    }


    @ParameterizedTest @MethodSource("factories")
    void testParseCodePoint(Factory fac) {
        //noinspection UnnecessaryUnicodeEscape
        int[] codePoints = "\0\t~\u007F\u00a3\u0418\u0939\u20ac\uD55c\uD800\uDF48"
                .codePoints().toArray();
        String lowercase = "\\u0000\\u0009\\u007e\\u007f\\u00a3\\u0418\\u0939\\u20ac\\ud55c\\U00010348";
        String uppercase = "\\u0000\\u0009\\u007E\\u007F\\u00A3\\u0418\\u0939\\u20AC\\uD55C\\U00010348";
        for (Rope r : fac.create(lowercase, uppercase)) {
            for (int i = 0, n = 0; i < r.len; i++, n++) {
                i = r.skipUntil(i, r.len, '\\');
                if (i == r.len) break;
                int code = 0;
                try {
                    code = r.parseCodePoint(i);
                } catch (Throwable t) {
                    fail("n="+n+", i="+i, t);
                }
                assertEquals(codePoints[n], code, "n="+n+", i="+i);
            }
        }
    }
}