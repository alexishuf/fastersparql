package com.github.alexishuf.fastersparql.model.rope;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.RopeWrapper.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RopeWrapperTest {

    static Stream<Arguments> test() {
        return Stream.of(
                arguments("", NONE, ""),
                arguments("<", NONE, "<"),
                arguments("<>", NONE, "<>"),
                arguments("\"", NONE, "\""),
                arguments("\"\"", NONE, "\"\""),
                arguments("\"\\\"", NONE, "\"\\\""),

                arguments("", LIT, "\"\""),
                arguments("a", LIT, "\"a\""),
                arguments("\"", LIT, "\"\"\""),

                arguments("", OPEN_LIT, "\""),
                arguments("a", OPEN_LIT, "\"a"),
                arguments("\"", OPEN_LIT, "\"\""),

                arguments("", CLOSE_LIT, "\""),
                arguments("a", CLOSE_LIT, "a\""),
                arguments("\"a", CLOSE_LIT, "\"a\""),
                arguments("\"", CLOSE_LIT, "\"\""),

                arguments("", IRI, "<>"),
                arguments("a", IRI, "<a>"),
                arguments("<", IRI, "<<>"),
                arguments(">", IRI, "<>>"),
                arguments("<>", IRI, "<<>>"),

                arguments("", OPEN_IRI, "<"),
                arguments("a", OPEN_IRI, "<a"),
                arguments(">", OPEN_IRI, "<>"),
                arguments("a>", OPEN_IRI, "<a>"),
                arguments("<a>", OPEN_IRI, "<<a>"),

                arguments("", CLOSE_IRI, ">"),
                arguments("a", CLOSE_IRI, "a>"),
                arguments("<", CLOSE_IRI, "<>"),
                arguments("<a", CLOSE_IRI, "<a>"),
                arguments(">", CLOSE_IRI, ">>")
        );
    }

    private static List<String> padded(String string) {
        return List.of('.' + string + '.',
                       '"' + string + '"',
                       '<' + string + '>',
                       '\\' + string + '"',
                       '\\' + string + '>');
    }

    @ParameterizedTest @MethodSource
    void test(String in, RopeWrapper w, String ex) {
        try (var r = PooledMutableRope.getWithCapacity(in.length()).append(in)) {
            var a = in.getBytes(UTF_8);
            var sb = new StringBuilder(in);
            var br = new FinalSegmentRope(ByteBuffer.wrap(in.getBytes(UTF_8)));

            assertEquals(ex, new String(w.toArray(in, 0, in.length()), UTF_8));
            assertEquals(ex, new String(w.toArray(sb, 0, in.length()), UTF_8));
            assertEquals(ex, new String(w.toArray(a, 0, r.len), UTF_8));
            assertEquals(ex, new String(w.toArray(r, 0, r.len), UTF_8));
            assertEquals(ex, new String(w.toArray(br, 0, r.len), UTF_8));

            if (w == NONE)
                assertSame(a, w.toArray(a, 0, r.len));

            for (String p : padded(in)) {
                var pa = p.getBytes(UTF_8);
                var psb = new StringBuilder(p);
                var pr = FinalSegmentRope.asFinal(p);
                var pbr = new FinalSegmentRope(ByteBuffer.wrap(p.getBytes(UTF_8)));
                int begin = 1, end = 1 + r.len, cEnd = 1 + in.length();

                assertEquals(ex, new String(w.toArray(p, begin, cEnd), UTF_8));
                assertEquals(ex, new String(w.toArray(psb, begin, cEnd), UTF_8));
                assertEquals(ex, new String(w.toArray(pa, begin, end), UTF_8));
                assertEquals(ex, new String(w.toArray(pr, begin, end), UTF_8));
                assertEquals(ex, new String(w.toArray(pbr, begin, end), UTF_8));
            }
        }
    }

    @FunctionalInterface
    interface Factory {
        RopeWrapper create(Object o, int begin, int end);
    }

    interface Converter {
        byte[] convert(Object o, int begin, int end);
    }

    private static final Factory forIri = new Factory() {
        @Override public RopeWrapper create(Object o, int begin, int end) { return RopeWrapper.forIri(o, begin, end); }
        @Override public String toString() { return "forIri"; }
    };
    private static final Factory forCloseIri  = new Factory() {
        @Override public RopeWrapper create(Object o, int begin, int end) { return RopeWrapper.forCloseIri(o, begin, end); }
        @Override public String toString() { return "forCloseIri"; }
    };
    private static final Factory forLit  = new Factory() {
        @Override public RopeWrapper create(Object o, int begin, int end) { return RopeWrapper.forLit(o, begin, end); }
        @Override public String toString() { return "forLit"; }
    };
    private static final Factory forOpenLit  = new Factory() {
        @Override public RopeWrapper create(Object o, int begin, int end) { return RopeWrapper.forOpenLit(o, begin, end); }
        @Override public String toString() { return "forOpenLit"; }
    };

    private static final Converter asIri = new Converter() {
        @Override public byte[] convert(Object o, int begin, int end) { return RopeWrapper.asIriU8(o, begin, end);}
        @Override public String toString() {return "asIri";}
    };
    private static final Converter asCloseIri = new Converter() {
        @Override public byte[] convert(Object o, int begin, int end) { return RopeWrapper.asCloseIriU8(o, begin, end);}
        @Override public String toString() {return "asCloseIri";}
    };
    private static final Converter asLit = new Converter() {
        @Override public byte[] convert(Object o, int begin, int end) { return RopeWrapper.asLitU8(o, begin, end);}
        @Override public String toString() {return "asLit";}
    };
    private static final Converter asOpenLit = new Converter() {
        @Override public byte[] convert(Object o, int begin, int end) { return RopeWrapper.asOpenLitU8(o, begin, end);}
        @Override public String toString() {return "asOpenLit";}
    };

    static Stream<Arguments> testFor() {
        String weirdIri = "http://example.org/?x=\"<>\"";
        return Stream.of(
                arguments("", forIri, asIri, IRI),
                arguments("a", forIri, asIri, IRI),
                arguments("<a", forIri, asIri, CLOSE_IRI),
                arguments("a>", forIri, asIri, OPEN_IRI),
                arguments("<a>", forIri, asIri, NONE),

                arguments(">", forIri, asIri, OPEN_IRI),
                arguments("<", forIri, asIri, CLOSE_IRI),
                arguments("<>", forIri, asIri, NONE),

                arguments(weirdIri, forIri, asIri, IRI),
                arguments("<"+weirdIri, forIri, asIri, CLOSE_IRI),
                arguments(weirdIri+">", forIri, asIri, OPEN_IRI),
                arguments("<"+weirdIri+">", forIri, asIri, NONE),

                arguments("", forCloseIri, asCloseIri, CLOSE_IRI),
                arguments("a", forCloseIri, asCloseIri, CLOSE_IRI),
                arguments("a>", forCloseIri, asCloseIri, NONE),
                arguments("<a>", forCloseIri, asCloseIri, NONE),

                arguments(weirdIri, forCloseIri, asCloseIri, CLOSE_IRI),
                arguments(weirdIri+">", forCloseIri, asCloseIri, NONE),
                arguments("<"+weirdIri+">", forCloseIri, asCloseIri, NONE),

                arguments("", forLit, asLit, LIT),
                arguments("a", forLit, asLit, LIT),
                arguments("a\"", forLit, asLit, OPEN_LIT),
                arguments("\"a", forLit, asLit, CLOSE_LIT),
                arguments("\"a\"", forLit, asLit, NONE),
                arguments("\"", forLit, asLit, CLOSE_LIT),
                arguments("\"\"", forLit, asLit, NONE),

                arguments("a\\\"", forLit, asLit, LIT),
                arguments("\\\"", forLit, asLit, LIT),
                arguments("\"a\\\"", forLit, asLit, CLOSE_LIT),
                arguments("\"\\\"", forLit, asLit, CLOSE_LIT),
                arguments("a\\\\\"", forLit, asLit, OPEN_LIT),

                arguments("", forOpenLit, asOpenLit, OPEN_LIT),
                arguments("a", forOpenLit, asOpenLit, OPEN_LIT),
                arguments("a\"", forOpenLit, asOpenLit, OPEN_LIT),
                arguments("a\\\"", forOpenLit, asOpenLit, OPEN_LIT),
                arguments("a\\\\\"", forOpenLit, asOpenLit, OPEN_LIT),
                arguments("\\\"", forOpenLit, asOpenLit, OPEN_LIT),
                arguments("\"", forOpenLit, asOpenLit, NONE),
                arguments("\"\"", forOpenLit, asOpenLit, NONE),
                arguments("\"", forOpenLit, asOpenLit, NONE)
        );
    }

    @ParameterizedTest @MethodSource
    void testFor(String in, Factory fac, Converter conv, RopeWrapper ex) {
        try (var r = PooledMutableRope.get().append(in)) {
            var sb = new StringBuilder(in);
            var a = in.getBytes(UTF_8);
            var br = new FinalSegmentRope(ByteBuffer.wrap(in.getBytes(UTF_8)));

            assertEquals(ex, fac.create(in, 0, in.length()));
            assertEquals(ex, fac.create(sb, 0, in.length()));
            assertEquals(ex, fac.create(a, 0, r.len));
            assertEquals(ex, fac.create(r, 0, r.len));
            assertEquals(ex, fac.create(br, 0, r.len));

            assertArrayEquals(ex.toArray(in, 0, in.length()), conv.convert(in, 0, in.length()));
            assertArrayEquals(ex.toArray(sb, 0, in.length()), conv.convert(sb, 0, in.length()));
            assertArrayEquals(ex.toArray(a, 0, r.len), conv.convert(a, 0, r.len));
            assertArrayEquals(ex.toArray(r, 0, r.len), conv.convert(r, 0, r.len));
            assertArrayEquals(ex.toArray(br, 0, r.len), conv.convert(br, 0, r.len));


            for (String p : padded(in)) {
                try (var pr = PooledMutableRope.get().append(p)) {
                    var psb = new StringBuilder(p);
                    var pa = p.getBytes(UTF_8);
                    var pbr = new FinalSegmentRope(ByteBuffer.wrap(p.getBytes(UTF_8)));

                    int begin = 1, cEnd = 1 + in.length(), end = 1 + r.len;

                    assertEquals(ex, fac.create(p, begin, cEnd));
                    assertEquals(ex, fac.create(psb, begin, cEnd));
                    assertEquals(ex, fac.create(pa, begin, end));
                    assertEquals(ex, fac.create(pr, begin, end));
                    assertEquals(ex, fac.create(pbr, begin, end));

                    assertArrayEquals(ex.toArray(p, begin, cEnd), conv.convert(p, begin, cEnd));
                    assertArrayEquals(ex.toArray(psb, begin, cEnd), conv.convert(psb, begin, cEnd));
                    assertArrayEquals(ex.toArray(pa, begin, end), conv.convert(pa, begin, end));
                    assertArrayEquals(ex.toArray(pr, begin, end), conv.convert(pr, begin, end));
                    assertArrayEquals(ex.toArray(pbr, begin, end), conv.convert(pbr, begin, end));
                }
            }
        }
    }

    static Stream<Arguments> testConverter() {
        return Stream.of(
                arguments("\"a\"", asLit, "\"a\""),
                arguments("\"a", asLit, "\"a\""),
                arguments("a\"", asLit, "\"a\""),
                arguments("a", asLit, "\"a\""),
                arguments("", asLit, "\"\""),
                arguments("\"", asLit, "\"\""),

                arguments("\"a\\\"\"", asLit, "\"a\\\"\""),
                arguments("\"a\\\"", asLit, "\"a\\\"\""),
                arguments("a\\\"", asLit, "\"a\\\"\""),
                arguments("\\\"", asLit, "\"\\\"\""),

                arguments("a", asOpenLit, "\"a"),
                arguments("\"a", asOpenLit, "\"a"),
                arguments("a\"", asOpenLit, "\"a\""),
                arguments("\"", asOpenLit, "\""),
                arguments("", asOpenLit, "\""),
                arguments("\\\"", asOpenLit, "\"\\\""),

                arguments("a", asIri, "<a>"),
                arguments("<a", asIri, "<a>"),
                arguments("a>", asIri, "<a>"),
                arguments("<a>", asIri, "<a>"),
                arguments("", asIri, "<>"),
                arguments("<", asIri, "<>"),
                arguments(">", asIri, "<>"),
                arguments("<>", asIri, "<>"),

                arguments("a", asCloseIri, "a>"),
                arguments("a>", asCloseIri, "a>"),
                arguments(">", asCloseIri, ">"),
                arguments("", asCloseIri, ">")
        );
    }

    @ParameterizedTest @MethodSource
    void testConverter(String in, Converter conv, String ex) {
        try (var r = PooledMutableRope.getWithCapacity(in.length()).append(in)) {
            var sb = new StringBuilder(in);
            var a = r.utf8;
            var br = new FinalSegmentRope(ByteBuffer.wrap(r.u8()));

            assertEquals(ex, new String(conv.convert(in, 0, in.length()), UTF_8));
            assertEquals(ex, new String(conv.convert(sb, 0, in.length()), UTF_8));
            assertEquals(ex, new String(conv.convert(r, 0, r.len), UTF_8));
            assertEquals(ex, new String(conv.convert(a, 0, r.len), UTF_8));
            assertEquals(ex, new String(conv.convert(br, 0, r.len), UTF_8));

            for (String p : padded(in)) {
                try (var pr = PooledMutableRope.get()) {
                    pr.append(p);
                    var psb = new StringBuilder(p);
                    var pa = pr.u8();
                    var pbr = new FinalSegmentRope(ByteBuffer.wrap(pr.u8()));
                    int begin = 1, end = 1 + r.len, cEnd = 1 + in.length();

                    assertEquals(ex, new String(conv.convert(p, begin, cEnd), UTF_8));
                    assertEquals(ex, new String(conv.convert(psb, begin, cEnd), UTF_8));
                    assertEquals(ex, new String(conv.convert(pr, begin, end), UTF_8));
                    assertEquals(ex, new String(conv.convert(pa, begin, end), UTF_8));
                    assertEquals(ex, new String(conv.convert(pbr, begin, end), UTF_8));
                }
            }
        }
    }

    static Stream<Arguments> testAppend() {
        List<Arguments> list = new ArrayList<>();
        for (String s : List.of("", "a", "ab")) {
            stream(values()).map(w -> arguments(w, s, 0, s.length())).forEach(list::add);
            stream(values()).map(w -> arguments(w, "."+s+".", 1, 1+s.length())).forEach(list::add);
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testAppend(RopeWrapper w, String in, int begin, int end) {
        String ex = "!" + new String(w.toArray(in, begin, end), UTF_8);
        try (var tmp = PooledMutableRope.get()) {
            assertEquals(ex, w.append(tmp.clear().append("!"), new StringBuilder(in), begin, end).toString());
            assertEquals(ex, w.append(tmp.clear().append("!"), Rope.asRope(in), begin, end).toString());
            assertEquals(ex, w.append(tmp.clear().append("!"), in.getBytes(UTF_8), begin, end).toString());


            if (begin == 0 && end == in.length()) {
                assertEquals(ex, w.append(tmp.clear().append("!"), new StringBuilder(in)).toString());
                assertEquals(ex, w.append(tmp.clear().append("!"), Rope.asRope(in)).toString());
                assertEquals(ex, w.append(tmp.clear().append("!"), in.getBytes(UTF_8)).toString());

                int bytesReq = ex.getBytes(UTF_8).length;
                assertEquals(ex, w.append(RopeFactory.make(bytesReq).add("!"), new StringBuilder(in)).take().toString());
                assertEquals(ex, w.append(RopeFactory.make(bytesReq).add("!"), Rope.asRope(in)).take().toString());
                assertEquals(ex, w.append(RopeFactory.make(bytesReq).add("!"), in.getBytes(UTF_8)).take().toString());
            }
        }
    }
}