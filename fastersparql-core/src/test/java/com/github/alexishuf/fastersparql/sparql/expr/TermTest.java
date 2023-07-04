package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.ByteRope.EMPTY;
import static com.github.alexishuf.fastersparql.model.rope.SharedRopes.*;
import static com.github.alexishuf.fastersparql.sparql.PrefixAssigner.CANON;
import static com.github.alexishuf.fastersparql.sparql.PrefixAssigner.NOP;
import static com.github.alexishuf.fastersparql.sparql.expr.Term.*;
import static java.lang.Integer.signum;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TermTest {
    static Stream<Arguments> testAsBool() {
        return Stream.of(
                arguments(true, Term.TRUE),
                arguments(false, Term.FALSE),
                arguments(false, Term.EMPTY_STRING),
                arguments(false, Term.lang("", "en")),
                arguments(true, plainString("0")),
                arguments(true, plainString("false")),
                arguments(true, Term.lang("a", "en")),
                arguments(false, Term.typed("0", DT_INT)),
                arguments(false, Term.typed("0", DT_FLOAT)),
                arguments(false, Term.typed("0", DT_DOUBLE)),
                arguments(false, Term.typed("0", DT_LONG)),
                arguments(false, Term.typed("0", DT_SHORT)),
                arguments(false, Term.typed("0", DT_BYTE)),
                arguments(false, Term.typed("0", DT_integer)),
                arguments(false, Term.typed("0", DT_decimal)),
                arguments(null, valueOf("<http://example.org/Alice>")),
                arguments(null, valueOf("_:asd"))
        );
    }

    @ParameterizedTest @MethodSource
    void testAsBool(Boolean expected, Term term) {
        if (expected == null)
            assertThrows(InvalidExprTypeException.class, term::asBool);
        else
            assertEquals(expected, term.asBool());
    }

    static Stream<Arguments> testAsNumber() {
        return Stream.of(
                arguments(null, Term.TRUE),
                arguments(null, Term.FALSE),
                arguments(null, plainString("")),
                arguments(null, plainString("0")),
                arguments(null, plainString("23")),
                arguments(null, plainString("-23.4e2")),
                arguments(0, Term.typed("0", DT_INT)),
                arguments(23, Term.typed("23", DT_INT)),
                arguments(-23, Term.typed("-23", DT_INT)),
                arguments(23, Term.typed("+23", DT_INT)),
                arguments(23L, Term.typed("23", DT_LONG)),
                arguments(23.0, Term.typed("23", DT_DOUBLE)),
                arguments(23.2, Term.typed("23.2", DT_DOUBLE)),
                arguments(-23e2, Term.typed("-23E2", DT_DOUBLE)),
                arguments(23.0f, Term.typed("23", DT_FLOAT)),
                arguments(Short.valueOf("23"), Term.typed("23", DT_SHORT)),
                arguments(Byte.valueOf("23"), Term.typed("23", DT_BYTE))
        );
    }

    @ParameterizedTest @MethodSource
    void testAsNumber(Number expected, Term lit) {
        assertEquals(expected, lit.asNumber());
    }

    static Stream<Arguments> testCompare() {
        Term i23 = Term.typed("23", DT_INT);
        Term l23 = Term.typed("23", DT_LONG);
        Term s23 = Term.typed("23", DT_SHORT);
        Term b23 = Term.typed("23", DT_BYTE);
        Term d23 = Term.typed("23.0", DT_DOUBLE);
        Term f23 = Term.typed("23.0", DT_FLOAT);
        Term D23 = Term.typed("23.0", DT_decimal);
        Term I23 = Term.typed("23", DT_integer);

        Term i24 = Term.typed("24", DT_INT);
        Term l24 = Term.typed("24", DT_LONG);
        Term s24 = Term.typed("24", DT_SHORT);
        Term b24 = Term.typed("24", DT_BYTE);
        Term d24 = Term.typed("24", DT_DOUBLE);
        Term f24 = Term.typed("24", DT_FLOAT);
        Term D24 = Term.typed("24", DT_decimal);
        Term I24 = Term.typed("24", DT_integer);
        return Stream.of(
                arguments(0, i23, i23),
                arguments(0, i23, l23),
                arguments(0, i23, s23),
                arguments(0, i23, b23),
                arguments(0, i23, d23),
                arguments(0, i23, f23),
                arguments(0, i23, D23),
                arguments(0, i23, I23),

                arguments(-1, i23, i24),
                arguments(-1, i23, l24),
                arguments(-1, i23, s24),
                arguments(-1, i23, b24),
                arguments(-1, i23, d24),
                arguments(-1, i23, f24),
                arguments(-1, i23, D24),
                arguments(-1, i23, I24)
        );
    }

    @SuppressWarnings("SimplifiableAssertion") @ParameterizedTest @MethodSource
    void testCompare(int expected, Term left, Term right) {
        assertEquals(expected, left.compareTo(right));
        assertEquals(-1 * expected, right.compareTo(left));
        if (expected == 0) {
            assertTrue(left.equals(right));
            assertTrue(right.equals(left));
            assertEquals(left, right);
        }
    }

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(false, valueOf("<a>"), valueOf("<b>")),
                arguments(true, valueOf("<a>"), valueOf("<a>")),
                arguments(false, valueOf("_:a"), valueOf("_:b")),
                arguments(true, valueOf("_:a"), valueOf("_:a")),
                arguments(false, Term.typed("a", DT_string),
                                 Term.typed("b", DT_string)),
                arguments(true, Term.typed("a", DT_string),
                                Term.typed("a", DT_string)),
                arguments(true, Term.lang("a", "en"),
                                Term.lang("a", "en")),
                arguments(false, Term.lang("a", "en"),
                                 Term.lang("a", "pt"))
        );
    }

    @ParameterizedTest @MethodSource
    void testEquals(boolean expected, Term l, Term r) {
        assertEquals(expected, l.equals(r));
        assertEquals(expected, r.equals(l));
    }

    static Stream<Arguments> testNT() {
        return Stream.of(
                arguments(valueOf("<a>"), "<a>"),
                arguments(valueOf("<http://example.org:8080/a?x=[1]^^>"), "<http://example.org:8080/a?x=[1]^^>"),
                arguments(valueOf("_:a"), "_:a"),
                arguments(valueOf("_:a23-"), "_:a23-"),
                arguments(valueOf("?x"), "?x"),
                arguments(valueOf("?2_x"), "?2_x"),
                arguments(valueOf("$2_x"), "$2_x"),
                arguments(Term.typed("a", DT_string), "\"a\""),
                arguments(Term.typed("23e2", DT_DOUBLE), "\"23e2\"^^<http://www.w3.org/2001/XMLSchema#double>"),
                arguments(Term.typed("+23.0", DT_decimal), "\"+23.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
                arguments(Term.typed("1 a ?x", DT_string), "\"1 a ?x\"")
        );
    }

    @ParameterizedTest @MethodSource
    void testNT(Term term, String expected) {
        assertEquals(expected, term.toString());
    }

    @Test void testTyped() {
        assertEquals("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                     Term.typed("\"1", DT_integer).toString());
        assertEquals("\"23.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
                     Term.typed("\"23.5", DT_decimal).toString());
    }

    @Test void testTypedUnquoted() {
        assertEquals("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                     Term.typed(1, DT_integer).toString());
        assertEquals("\"23\"^^<http://www.w3.org/2001/XMLSchema#int>",
                     Term.typed(23, DT_INT).toString());

        assertEquals("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                Term.typed("1", DT_integer).toString());
        assertEquals("\"23\"^^<http://www.w3.org/2001/XMLSchema#int>",
                Term.typed("23", DT_INT).toString());
        assertEquals("\"23.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
                     Term.typed("23.5", DT_decimal).toString());

        assertEquals("\"\"", Term.typed("", DT_string).toString());
    }

    @Test void testTypedInsertsLeadingQuote() {
        assertEquals("\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                     Term.typed("0", DT_integer).toString());
        assertEquals("\"23.7\"^^<http://www.w3.org/2001/XMLSchema#decimal>",
                     Term.typed("23.7", DT_decimal).toString());
        assertEquals("\"\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON>",
                     Term.typed("", DT_JSON).toString());

        assertEquals("\"bob\"", Term.typed("bob", DT_string).toString());
        assertEquals("\"\"", Term.typed("", DT_string).toString());
        assertEquals("\"a\"", Term.typed("a", DT_string).toString());
        assertEquals("\"ab\"", Term.typed("ab", DT_string).toString());
    }

    @Test void testTypedOmitsStringDatatype() {
        assertEquals("\"bob\"", Term.typed("\"bob", DT_string).toString());
        assertEquals("\"\"", Term.typed("\"", DT_string).toString());
    }

    @Test void testTypedClosedQuote() {
        assertThrows(AssertionError.class, () -> Term.typed("\"0\"", DT_integer));
    }

    @Test void testTypedBadSuffix() {
        assertEquals(Term.plainString("0"), Term.wrap(new ByteRope("\"0\""), EMPTY));
        assertEquals(Term.plainString("0"), Term.wrap(new ByteRope("\"0\""), null));
        assertThrows(IllegalArgumentException.class, () -> Term.wrap(new ByteRope("\"0\""), P_XSD));
    }

    @Test void testPrefixed() {
        assertEquals("<http://www.w3.org/2001/XMLSchema#integer>",
                     Term.wrap(P_XSD, new ByteRope("integer>")).toString());
        assertEquals(XSD_INTEGER, Term.wrap(P_XSD, new ByteRope("integer>")));
        assertSame(XSD_INTEGER, Term.wrap(P_XSD, new ByteRope("integer>")));
    }

    @Test void testPrefixedRopeInterns() {
        assertSame(XSD_INTEGER, Term.wrap(P_XSD, SegmentRope.of("integer>")));
        assertSame(XSD_INTEGER, Term.wrap(P_XSD, SegmentRope.of("xsd:integer>").sub(4, 12)));

        assertSame(XSD, Term.wrap(P_XSD, SegmentRope.of(">")));
        assertSame(XSD, Term.wrap(P_XSD, SegmentRope.of("xsd:>").sub(4, 5)));

        assertSame(RDF_TYPE, Term.wrap(P_RDF, SegmentRope.of("type>")));
        assertSame(RDF_TYPE, Term.wrap(P_RDF, SegmentRope.of(".type>").sub(1, 6)));
    }


    @Test void testPrefixedInternShortLocal() {
        var sh = SHARED_ROPES.internPrefixOf(SegmentRope.of("<http://www.example.org/ns#>"), 0, 28);
        assertEquals(SegmentRope.of("<http://www.example.org/ns#"), sh);

        assertSame(CLOSE_IRI, Term.wrap(sh, SegmentRope.of(">")).local());
        assertSame(CLOSE_IRI, Term.wrap(sh, SegmentRope.of(">.").sub(0, 1)).local());
        assertSame(CLOSE_IRI, Term.wrap(sh, SegmentRope.of(".>,").sub(1, 2)).local());

        Term xy = Term.wrap(sh, SegmentRope.of("xy>"));
        assertSame(xy.local(), Term.wrap(sh, SegmentRope.of("xy>")).local());
        assertSame(xy.local(), Term.wrap(sh, SegmentRope.of("xy>.").sub(0, 3)).local());
        assertSame(xy.local(), Term.wrap(sh, SegmentRope.of(":xy>").sub(1, 4)).local());

        Term x = Term.wrap(sh, SegmentRope.of("x>"));
        assertSame(x.local(), Term.wrap(sh, SegmentRope.of("x>")).local());
        assertSame(x.local(), Term.wrap(sh, SegmentRope.of("x>.").sub(0, 2)).local());
        assertSame(x.local(), Term.wrap(sh, SegmentRope.of(":x>").sub(1, 3)).local());
    }

    @Test void testLang() {
        assertEquals("\"bob\"@en", Term.lang("bob", "en").toString());
        assertEquals("\"bob\"@en-US", Term.lang("bob", "en-US").toString());
        assertEquals("\"\"@en", Term.lang("", "en").toString());
        assertEquals("\"\"@en", Term.lang("\"", "en").toString());
        assertEquals("\"\\\"\"@en", Term.lang("\\\"", "en").toString());
    }

    @Test void testIri() {
        for (var iri : List.of("http://example.org/Bob", "http://example.org/1", "http://example.org/aB")) {
            String wrapped = "<" + iri + ">";
            assertEquals(wrapped, requireNonNull(splitAndWrap(new ByteRope(wrapped))).toString());
            assertEquals(wrapped, Term.valueOf(wrapped).toString());
            assertEquals(wrapped, Term.iri(wrapped).toString());
            assertEquals(wrapped, Term.iri("<"+iri).toString());
            assertEquals(wrapped, Term.iri(iri+">").toString());
            assertEquals(wrapped, Term.iri(iri).toString());
        }
    }

    @Test void testWrap() {
        String bob = "\"bob\"";
        var r = new ByteRope(bob);
        Term term = wrap(r, null);
        assertEquals(bob, term.toString());
        assertEquals(Term.Type.LIT, term.type());

        r.u8()[1] = 'r';
        assertEquals("\"rob\"", term.toString()); // change reflects in term
    }

    @Test void testPlain() {
        assertEquals("\"quoted\"", plainString("\"quoted\"").toString());
        assertEquals("\"\\n\"", plainString("\"\\n\"").toString());
        assertEquals("\"bob\"", plainString("bob").toString());
        assertEquals("\"a\"", plainString("a").toString());
        assertEquals("\"23\"", plainString("23").toString());
    }

    static Stream<Arguments> testValueOf() {
        return Stream.of(
                arguments("_:bn", null),
                arguments("_:", null),
                arguments("\"plain\"", null),
                arguments("\"\"", null),
                arguments("\"a\"^^<http://www.w3.org/2001/XMLSchema#string>", "\"a\""),
                arguments("\"\"^^<http://www.w3.org/2001/XMLSchema#string>", "\"\""),
                arguments("\"a\"@en", null),
                arguments("\"\"@en-US", null),
                arguments("\"0\"^^<http://www.w3.org/2001/XMLSchema#integer>", null),
                arguments("\"-2.3E+09\"^^<http://www.w3.org/2001/XMLSchema#double>", null),
                arguments("<http://www.w3.org/2001/XMLSchema#integer>", null),

                arguments("\"", "ERROR"),
                arguments("<", "ERROR"),
                arguments("<>", "<>"),
                arguments("*", "ERROR"),
                arguments("_", "ERROR"),
                arguments("_X", "ERROR"),
                arguments(":Alice", "ERROR"),
                arguments("23", "ERROR"),
                arguments("false", "ERROR")
        );
    }

    @ParameterizedTest @MethodSource
    void testValueOf(String in, String ex) {
        ex = ex == null ? in : ex;
        byte[] u8 = in.getBytes(UTF_8);
        ByteRope br = new ByteRope(u8);
        SegmentRope padded = new SegmentRope(ByteBuffer.wrap(("\""+in+"\"").getBytes(UTF_8)));

        if (ex.equals("ERROR")) {
            assertThrows(Throwable.class, () -> Term.valueOf(br));
            assertThrows(Throwable.class, () -> Term.valueOf(in));
            assertThrows(Throwable.class, () -> Term.valueOf(br, 0, u8.length));
            assertThrows(Throwable.class, () -> Term.valueOf(padded, 1, 1+u8.length));
        } else {
            assertEquals(ex, Term.valueOf(br).toString());
            assertEquals(ex, Term.valueOf(in).toString());
            assertEquals(ex, Term.valueOf(br, 0, u8.length).toString());
            assertEquals(ex, Term.valueOf(padded, 1, 1+u8.length).toString());
        }
    }

    @Test void testValueOfNull() {
        //noinspection RedundantCast
        assertNull(Term.valueOf((Rope)null));
        assertNull(Term.valueOf(null, 0, 0));
        assertNull(Term.valueOf(null, 0, 23));
        //noinspection RedundantCast
        assertNull(Term.valueOf((CharSequence) null));
        //noinspection RedundantCast
        assertNull(Term.valueOf((String) null));

        assertNull(Term.valueOf(EMPTY));
        assertNull(Term.valueOf(EMPTY, 0, 0));
        assertNull(Term.valueOf(SegmentRope.of("a"), 1, 1));
        assertNull(Term.valueOf(SegmentRope.of("a"), 0, 0));
        assertNull(Term.valueOf(SegmentRope.of("a"), 23, 23));
        assertNull(Term.valueOf(new StringBuilder()));
        assertNull(Term.valueOf(""));
    }

     static Stream<Arguments> testMake() {
        return Stream.of(
                arguments(DT_string, "\"", EMPTY_STRING),
                arguments(DT_integer, "\"1", Term.typed(1, DT_integer)),
                arguments(DT_SHORT, "\"23", Term.typed(23, DT_SHORT)),
                arguments(DT_integer, "\"-23", Term.typed(-23, DT_integer)),
                arguments(DT_INT, "\"456", Term.typed(456, DT_INT)),
                arguments(DT_unsignedInt, "\"-917", Term.typed(-917, DT_unsignedInt)),
                arguments(DT_decimal, "\"23.5", Term.typed("23.5", DT_decimal)),
                arguments(DT_DOUBLE, "\"-1.2e+2", Term.typed("-1.2e+2", DT_DOUBLE)),

                arguments(EMPTY, "\"\"", EMPTY_STRING),
                arguments(EMPTY, "\"a\"", plainString("a")),
                arguments(EMPTY, "\" \"", plainString(" ")),
                arguments(EMPTY, "\"as\"", plainString("as")),
                arguments(EMPTY, "\"asd\"", plainString("asd")),
                arguments(EMPTY, "\"Alice\"", plainString("Alice")),

                arguments(EMPTY, "\"\"@en", Term.lang("", "en")),
                arguments(EMPTY, "\"\"@pt-BR", Term.lang("", "pt-BR")),

                arguments(P_XSD, "string>", Term.XSD_STRING),
                arguments(P_XSD, "short>", Term.XSD_SHORT),
                arguments(P_XSD, "anyURI>", Term.XSD_ANYURI),
                arguments(P_RDF, "type>", Term.RDF_TYPE),
                arguments(P_RDF, "XMLLiteral>", Term.RDF_XMLLITERAL)
        );
    }

    @ParameterizedTest @MethodSource void testMake(SegmentRope sh, String local, Term expected) {
        var localRope = new ByteRope(local);
        var localRope2 = new ByteRope("("+local+")").sub(1, 1+local.length());
        boolean suffix = local.charAt(0) == '"';
        Term term = wrap(suffix ? localRope : sh, suffix ? sh : localRope);
        Term term2 = wrap(suffix ? localRope2 : sh, suffix ? sh : localRope2);
        assertEquals(expected, term);
        assertEquals(expected, term2);

        if (sh == null) {
            assertEquals(local, expected.toString());
        } else if (sh == DT_string) {
            assertEquals(local+"\"", requireNonNull(term).toString());
        } else {
            assertEquals(suffix ? local+sh : sh+local, term.toString());
        }
    }

    @Test void testMakeInvalid() {
        assertThrows(Throwable.class, () -> Term.wrap(DT_string, new ByteRope("\"asd")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, new ByteRope("\"")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, new ByteRope(".")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, new ByteRope("<\"")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, new ByteRope("<\".")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, new ByteRope(">")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, new ByteRope(".")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, new ByteRope("<.")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, new ByteRope(">..")));

        assertThrows(Throwable.class, () -> new Term(P_XSD, new ByteRope(">"), true));
    }

    static Stream<Arguments> testWrapInterns() {
        return Stream.of(
                arguments(null, new ByteRope("\"\""), EMPTY_STRING),
                arguments(P_XSD, new ByteRope("anyURI>"), XSD_ANYURI),
                arguments(P_XSD, new ByteRope(".anyURI>)").sub(1, 8), XSD_ANYURI),
                arguments(P_XSD, new ByteRope("unsignedInt>"), XSD_UNSIGNEDINT),
                arguments(P_RDF, new ByteRope("type>"), RDF_TYPE),
                arguments(P_RDF, new ByteRope("\"type>)").sub(1, 6), RDF_TYPE),
                arguments(P_RDF, new ByteRope("Property>"), RDF_PROPERTY),
                arguments(null, new ByteRope("\"1\""), Term.wrap(null, new ByteRope("\"1\""))),
                arguments(EMPTY, new ByteRope("\"aZ\""), Term.wrap(null, new ByteRope("\"aZ\"")))
        );
    }

    @ParameterizedTest @MethodSource void testWrapInterns(SegmentRope sh, SegmentRope local,
                                                          Term expected) {
        SegmentRope fst, snd;
        if (local.get(0) == '"') { fst = local; snd =    sh; }
        else                     { fst =    sh; snd = local; }

        assertEquals(expected, Term.wrap(fst, snd));
        assertSame(expected, Term.wrap(fst, snd));
    }

    @Test
    void testMakeInternsIriLocal() {
        var iri = SegmentRope.of("<http://www.example.org/ns#>");
        SegmentRope sh = SHARED_ROPES.internPrefixOf(iri, 0, iri.len);
        assertNotNull(sh);

        assertSame(CLOSE_IRI, requireNonNull(wrap(sh, new ByteRope(">")).local()));
        Term one = wrap(sh, new ByteRope("1>"));
        Term ab = wrap(sh, new ByteRope("ab>"));
        assertNotNull(one);
        assertNotNull(ab);
        assertSame(one.local(), requireNonNull(wrap(sh, new ByteRope("1>")).local()));
        assertSame(ab.local(), requireNonNull(wrap(sh, new ByteRope("ab>")).local()));
    }

    static Stream<Arguments> testValueOfReversible() {
        return Stream.of(
                testAsBool().map(a -> a.get()[1].toString()),
                testAsNumber().map(a -> a.get()[1].toString()),
                testCompare().map(a -> a.get()[1].toString()),
                testCompare().map(a -> a.get()[2].toString()),
                testEquals().map(a -> a.get()[1].toString()),
                testEquals().map(a -> a.get()[2].toString()),
                testNT().map(a -> a.get()[1].toString())
        ).flatMap(s -> s).distinct().map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testValueOfReversible(String in) {
        byte[] utf8 = in.getBytes(UTF_8);
        assertEquals(in, valueOf(new ByteRope(utf8)).toString());
        Term wrapped = valueOf(SegmentRope.of("'\"", new ByteRope(utf8), "<"), 2, 2+utf8.length);
        assertEquals(in, wrapped.toString());
        assertEquals(in, valueOf(in).toString());
    }

    @Test
    void testVar() {
        for (Term t : List.of(valueOf("?x"), valueOf("$x"))) {
            assertEquals(Type.VAR, t.type());
            assertEquals("x", requireNonNull(t.name()).toString());
        }
        for (Term t : List.of(valueOf(SegmentRope.of(", ?1"), 2, 4),
                valueOf(SegmentRope.of("_:$1"), 2, 4))) {
            assertEquals(Type.VAR, t.type());
            assertEquals("1", requireNonNull(t.name()).toString());
        }
        for (Term t : List.of(valueOf(SegmentRope.of(",?test123"), 1, 9),
                valueOf(SegmentRope.of("$test123")))) {
            assertEquals(Type.VAR, t.type());
            assertEquals("test123", requireNonNull(t.name()).toString());
        }

        for (Term t : List.of(valueOf("$test"), valueOf(SegmentRope.of("$test")),
                              valueOf(new SegmentRope(ByteBuffer.wrap(",$test".getBytes(UTF_8))), 1, 6))) {
            assertEquals("$test", t.toString());
            assertEquals("$test", t.toString(0, 5));
            assertEquals("test", requireNonNull(t.name()).toString());
        }
    }

    static Stream<Arguments> testEscapedLexical() {
        return Stream.of(
                arguments("\"a\"", "a"),
                arguments("\"ab\"", "ab"),
                arguments("\"123456789\"", "123456789"),
                arguments("\"a b\"", "a b"),
                arguments("\" \"", " "),
                arguments("\"\"", ""),
                arguments("\"a\\\"b\"", "a\\\"b"),
                arguments("\"\\\"\"", "\\\""),
                arguments("\"\\\\\"", "\\\\"),

                arguments("\"23\"^^<http://www.w3.org/2001/XMLSchema#integer>", "23"),
                arguments("\"-2.3e+02\"^^<http://www.w3.org/2001/XMLSchema#double>", "-2.3e+02")
        );
    }

    @ParameterizedTest @MethodSource
    void testEscapedLexical(String in, String ex) {
        assertEquals(ex, requireNonNull(valueOf(SegmentRope.of(in)).escapedLexical()).toString());
        assertEquals(ex, requireNonNull(valueOf(in).escapedLexical()).toString());
        var padded = SegmentRope.of("\"" + in + "\"");
        assertEquals(ex, requireNonNull(valueOf(padded, 1, 1+in.length()).escapedLexical()).toString());

    }

    static Stream<Arguments> testToSparql() {
        var customMap = new RopeArrayMap();
        customMap.put(new ByteRope("<http://example.org/"), EMPTY);
        customMap.put(new ByteRope("<http://xmlns.com/foaf/0.1/"), Rope.of("foaf"));
        var custom = new PrefixAssigner(customMap);
        SegmentRope foaf = SHARED_ROPES.internPrefixOf(SegmentRope.of("<http://xmlns.com/foaf/0.1/>"), 0, 28);
        assertNotNull(foaf);

        return Stream.of(
                arguments(NOP, EMPTY, "_:bn", "_:bn"),
                arguments(NOP, EMPTY, "\"bob\"", "\"bob\""),
                arguments(NOP, EMPTY, "\"bob\"@en", "\"bob\"@en"),
                arguments(NOP, EMPTY, "\"bob\"@en-US", "\"bob\"@en-US"),
                arguments(NOP, EMPTY, "\"\\\"\"", "\"\\\"\""),
                arguments(NOP, EMPTY, "<>", "<>"),
                arguments(NOP, EMPTY, "<rel>", "<rel>"),
                arguments(NOP, EMPTY, "<http://example.org/>", "<http://example.org/>"),

                arguments(NOP, P_XSD, "int>", "<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(NOP, P_RDF, "object>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#object>"),
                arguments(NOP, P_RDF, "type>", "a"),

                arguments(CANON, P_XSD, "int>", "xsd:int"),
                arguments(CANON, P_RDF, "object>", "rdf:object"),
                arguments(CANON, P_RDF, "type>", "a"),

                arguments(NOP, DT_integer, "\"7", "7"),
                arguments(NOP, DT_integer, "\"23", "23"),
                arguments(NOP, DT_integer, "\"-23", "-23"),
                arguments(NOP, DT_decimal, "\"2.3", "2.3"),
                arguments(NOP, DT_decimal, "\"1.999", "1.999"),
                arguments(NOP, DT_decimal, "\"-0.33", "-0.33"),
                arguments(NOP, DT_DOUBLE, "\"+2.3e-02", "+2.3e-02"),
                arguments(NOP, DT_DOUBLE, "\"2.3", "\"2.3\"^^<http://www.w3.org/2001/XMLSchema#double>"),

                arguments(NOP,   DT_INT, "\"23", "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(CANON, DT_INT, "\"23", "\"23\"^^xsd:int"),

                arguments(NOP, foaf, "knows>", "<http://xmlns.com/foaf/0.1/knows>"),
                arguments(custom, foaf, "knows>", "foaf:knows")
        );
    }

    @ParameterizedTest @MethodSource
    void testToSparql(PrefixAssigner assigner, SegmentRope sh, String local, String expected) {
        for (String padLeft : List.of("", "<", "\"<")) {
            for (String padRight : List.of("", "\"", ">", ">\"")) {
                var ctx = "padLeft=" + padLeft + ", padRight=" + padRight
                        + ", sh=" + sh + ", local=" + local;
                int off = padLeft.length(), len = local.getBytes(UTF_8).length;
                ByteRope actual = new ByteRope();
                byte[] u8 = (padLeft + local + padRight).getBytes(UTF_8);
                MemorySegment seg = MemorySegment.ofArray(u8);
                Term.toSparql(actual, assigner, sh, seg, u8, off, len,
                              sh.len > 0 && sh.get(0) == '"');
                assertEquals(expected, actual.toString(), ctx);
            }
        }
    }

    @SuppressWarnings("UnnecessaryUnicodeEscape") static Stream<Arguments> testUnescapedLexical() {
        return Stream.of(
                arguments("\"~\"", 1, "~"),
                arguments("\"\"", 0, ""),
                arguments("\"\\u20b2\"", 3, "\u20b2"),
                arguments("\" \\u0418 \"", 2+2, " \u0418 "),
                arguments("\".\\U00000939.\"", 3+2, ".\u0939."),
                arguments("\"\\\"\"", 1, "\""),
                arguments("\"\\t\\r\\n\"", 3, "\t\r\n"),
                arguments("\"\\U00010348\"", 4, "\uD800\uDF48")
        );
    }

    @ParameterizedTest @MethodSource("testUnescapedLexical")
    void testUnescapedLexicalSize(String in, int expected, String ignored) {
        Term term = valueOf(in);
        assertEquals(expected, term.unescapedLexicalSize());
    }

    @ParameterizedTest @MethodSource
    void testUnescapedLexical(String in, int exSize, String ex) {
        Term term = valueOf(in);
        ByteRope dest = new ByteRope().append('@');
        int n = term.unescapedLexical(dest);
        assertEquals(dest.len, n+1);
        assertEquals("@"+ex, dest.toString());
    }

    static Stream<Arguments> testTolerantNumericComparison() {
        return Stream.of(
                arguments("\"1\"^^xsd:int", "\"1\"^^xsd:int", 0),
                arguments("\"2\"^^xsd:int", "\"1\"^^xsd:int", 1),
                arguments("\"10\"^^xsd:unsignedShort", "\"2\"^^xsd:integer", 1),
                arguments("\"1.0\"^^xsd:float", "\"1.0\"^^xsd:float", 0),
                arguments("\"2.0\"^^xsd:decimal", "\"1.0\"^^xsd:float", 1),
                arguments("\"3.0\"^^xsd:double", "\"1.0\"^^xsd:decimal", 1),
                arguments("\"1.00\"^^xsd:decimal", "\"1.0\"^^xsd:double", 0),
                arguments("\"1.0\"^^xsd:float", "\"1\"^^xsd:decimal", 0),
                arguments("\"1.000\"^^xsd:float", "\"1.0\"^^xsd:double", 0),
                arguments("\"52.5167\"^^xsd:float", "\"52.5166666666\"^^xsd:float", 0),
                arguments("\"52.5167\"^^xsd:float", "\"52.51666666\"^^xsd:double", 0),
                arguments("\"52.5167\"^^xsd:float", "\"52.51666\"^^xsd:decimal", 1),
                arguments("\"52.5167\"^^xsd:float", "\"52.51666666\"^^xsd:decimal", 0),
                arguments("\"52.5167\"^^xsd:float", "\"52.5166\"^^xsd:float", 1),
                arguments("\"52.5167\"^^xsd:float", "\"52.5168\"^^xsd:float", -1),
                arguments("\"52.5167\"^^xsd:float", "\"52.51655\"^^xsd:float", 1),
                arguments("\"52.5167\"^^xsd:double", "\"52.51688\"^^xsd:float", -1),
                arguments("\"52.5167\"^^xsd:float", "\"52.516666666666666\"^^xsd:float", 0),
                arguments("\"52.3167\"^^xsd:float", "\"52.31666666666667\"^^xsd:float", 0),
                arguments("\"12.4833\"^^xsd:double", "\"12.483333333333333\"^^xsd:double", 0),
                arguments("\"12.4833\"^^xsd:float", "\"12.483333333333333\"^^xsd:float", 0),
                arguments("\"12.4833\"^^xsd:double", "\"12.483333333333333\"^^xsd:float", 0),
                arguments("\"23.7167\"^^xsd:float", "\"23.716666666666665\"^^xsd:float", 0),
                arguments("\"23.7167\"^^xsd:float", "\"23.716666666666664\"^^xsd:float", 0),
                arguments("\"23.7167\"^^xsd:float", "\"23.716666666666668\"^^xsd:float", 0),
                arguments("\"-0.116667\"^^xsd:double", "\"-0.11666666666666667\"^^xsd:double", 0),
                arguments("\"39.9167\"^^xsd:float", "\"39.916666666666664\"^^xsd:float", 0)

        );
    }

    @ParameterizedTest @MethodSource
    void testTolerantNumericComparison(String lStr, String rStr, int expected) {
        Term l = Objects.requireNonNull(Term.termList(lStr).get(0));
        Term r = Objects.requireNonNull(Term.termList(rStr).get(0));
        assertEquals( signum(expected), signum(l.compareTo(r)));
        assertEquals(-signum(expected), signum(r.compareTo(l)));
        assertEquals(expected == 0, l.equals(r));
        assertEquals(expected == 0, r.equals(l));
        assertTrue(l.hashCode() != 0);
        assertTrue(r.hashCode() != 0);
        if (expected == 0)
            assertEquals(l.hashCode(), r.hashCode());
    }
}