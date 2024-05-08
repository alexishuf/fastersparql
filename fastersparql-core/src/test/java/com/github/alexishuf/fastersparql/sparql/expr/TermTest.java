package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.RopeArrayMap;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
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
        var zero = asFinal("\"0\"");
        assertEquals(Term.plainString("0"), Term.wrap(zero, FinalSegmentRope.EMPTY));
        assertEquals(Term.plainString("0"), Term.wrap(zero, null));
        assertThrows(IllegalArgumentException.class, () -> Term.wrap(zero, P_XSD));
    }

    @Test void testPrefixed() {
        FinalSegmentRope local = asFinal("integer>");
        assertEquals("<http://www.w3.org/2001/XMLSchema#integer>",
                     Term.wrap(P_XSD, local).toString());
        assertEquals(XSD_INTEGER, Term.wrap(P_XSD, local));
        assertSame(XSD_INTEGER, Term.wrap(P_XSD, local));
    }

    @Test void testPrefixedRopeInterns() {
        assertSame(XSD_INTEGER, Term.wrap(P_XSD, asFinal("integer>")));
        assertSame(XSD_INTEGER, Term.wrap(P_XSD, asFinal("xsd:integer>").sub(4, 12)));

        assertSame(XSD, Term.wrap(P_XSD, asFinal(">")));
        assertSame(XSD, Term.wrap(P_XSD, asFinal("xsd:>").sub(4, 5)));

        assertSame(RDF_TYPE, Term.wrap(P_RDF, asFinal("type>")));
        assertSame(RDF_TYPE, Term.wrap(P_RDF, asFinal(".type>").sub(1, 6)));
    }


    @Test void testPrefixedInternShortLocal() {
        var sh = SHARED_ROPES.internPrefixOf(asFinal("<http://www.example.org/ns#>"), 0, 28);
        assertEquals(asFinal("<http://www.example.org/ns#"), sh);

        assertSame(CLOSE_IRI, Term.wrap(sh, asFinal(">")).local());
        assertSame(CLOSE_IRI, Term.wrap(sh, asFinal(">.").sub(0, 1)).local());
        assertSame(CLOSE_IRI, Term.wrap(sh, asFinal(".>,").sub(1, 2)).local());

        Term xy = Term.wrap(sh, asFinal("xy>"));
        assertSame(xy.local(), Term.wrap(sh, asFinal("xy>")).local());
        assertSame(xy.local(), Term.wrap(sh, asFinal("xy>.").sub(0, 3)).local());
        assertSame(xy.local(), Term.wrap(sh, asFinal(":xy>").sub(1, 4)).local());

        Term x = Term.wrap(sh, asFinal("x>"));
        assertSame(x.local(), Term.wrap(sh, asFinal("x>")).local());
        assertSame(x.local(), Term.wrap(sh, asFinal("x>.").sub(0, 2)).local());
        assertSame(x.local(), Term.wrap(sh, asFinal(":x>").sub(1, 3)).local());
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
            assertEquals(wrapped, requireNonNull(splitAndWrap(asFinal(wrapped))).toString());
            assertEquals(wrapped, Term.valueOf(wrapped).toString());
            assertEquals(wrapped, Term.iri(wrapped).toString());
            assertEquals(wrapped, Term.iri("<"+iri).toString());
            assertEquals(wrapped, Term.iri(iri+">").toString());
            assertEquals(wrapped, Term.iri(iri).toString());
        }
    }

    @Test void testWrap() {
        String bob = "\"bob\"";
        Term term;
        try (var mutable = PooledMutableRope.get().append(bob)) {
            term = wrap(mutable, null);
            assertEquals(bob, term.toString());
            assertEquals(Type.LIT, term.type());

            mutable.u8()[1] = 'r';
            assertEquals(bob, term.toString()); // change does NOT reflect in term
        }
        assertEquals(bob, term.toString()); // still valid
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
        FinalTerm fromMutable = null;
        try (var mutable = PooledMutableRope.get().append(u8)) {
            var padded = new FinalSegmentRope(ByteBuffer.wrap(("\"" + in + "\"").getBytes(UTF_8)));

            if (ex.equals("ERROR")) {
                assertThrows(Throwable.class, () -> Term.valueOf(mutable));
                assertThrows(Throwable.class, () -> Term.valueOf(in));
                assertThrows(Throwable.class, () -> Term.valueOf(mutable, 0, u8.length));
                assertThrows(Throwable.class, () -> Term.valueOf(padded, 1, 1 + u8.length));
            } else {
                fromMutable = valueOf(mutable);
                assertEquals(ex, fromMutable.toString());
                assertEquals(ex, Term.valueOf(in).toString());
                assertEquals(ex, Term.valueOf(mutable, 0, u8.length).toString());
                assertEquals(ex, Term.valueOf(padded, 1, 1 + u8.length).toString());
            }
        }
        if (fromMutable != null)
            assertEquals(ex, fromMutable.toString()); // must remain valid after br.close
    }

    @Test void testValueOfNull() {
        assertNull(Term.valueOf((Rope)null));
        assertNull(Term.valueOf(null, 0, 0));
        assertNull(Term.valueOf(null, 0, 23));
        assertNull(Term.valueOf((CharSequence) null));
        assertNull(Term.valueOf((String) null));

        assertNull(Term.valueOf(FinalSegmentRope.EMPTY));
        assertNull(Term.valueOf(FinalSegmentRope.EMPTY, 0, 0));
        assertNull(Term.valueOf(asFinal("a"), 1, 1));
        assertNull(Term.valueOf(asFinal("a"), 0, 0));
        assertNull(Term.valueOf(asFinal("a"), 23, 23));
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

                arguments(FinalSegmentRope.EMPTY, "\"\"", EMPTY_STRING),
                arguments(FinalSegmentRope.EMPTY, "\"a\"", plainString("a")),
                arguments(FinalSegmentRope.EMPTY, "\" \"", plainString(" ")),
                arguments(FinalSegmentRope.EMPTY, "\"as\"", plainString("as")),
                arguments(FinalSegmentRope.EMPTY, "\"asd\"", plainString("asd")),
                arguments(FinalSegmentRope.EMPTY, "\"Alice\"", plainString("Alice")),

                arguments(FinalSegmentRope.EMPTY, "\"\"@en", Term.lang("", "en")),
                arguments(FinalSegmentRope.EMPTY, "\"\"@pt-BR", Term.lang("", "pt-BR")),

                arguments(P_XSD, "string>", Term.XSD_STRING),
                arguments(P_XSD, "short>", Term.XSD_SHORT),
                arguments(P_XSD, "anyURI>", Term.XSD_ANYURI),
                arguments(P_RDF, "type>", Term.RDF_TYPE),
                arguments(P_RDF, "XMLLiteral>", Term.RDF_XMLLITERAL)
        );
    }

    @ParameterizedTest @MethodSource void testMake(SegmentRope sh, String local, Term expected) {
        boolean suffix;
        Term term, term2;
        try (var localRope = PooledMutableRope.get().append(local);
             var localRope2Outer = PooledMutableRope.get()) {
            localRope2Outer.append('(').append(local).append(')');
            var localRope2 = localRope2Outer.sub(1, 1+local.length());
            suffix = local.charAt(0) == '"';
            term = wrap(suffix ? localRope : sh, suffix ? sh : localRope);
            term2 = wrap(suffix ? localRope2 : sh, suffix ? sh : localRope2);
        } // mutable ropes are invalidated, but terms must remain valid
        assertEquals(expected, term);
        assertEquals(expected, term2);

        if (sh == null)
            assertEquals(local, expected.toString());
        else if (sh == DT_string)
            assertEquals(local+"\"", requireNonNull(term).toString());
        else
            assertEquals(suffix ? local+sh : sh+local, term.toString());

    }

    @Test void testMakeInvalid() {
        assertThrows(Throwable.class, () -> Term.wrap(DT_string, FinalSegmentRope.asFinal("\"asd")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, FinalSegmentRope.asFinal("\"")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, FinalSegmentRope.asFinal(".")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, FinalSegmentRope.asFinal("<\"")));
        assertThrows(Throwable.class, () -> Term.wrap(P_XSD, FinalSegmentRope.asFinal("<\".")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, FinalSegmentRope.asFinal(">")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, FinalSegmentRope.asFinal(".")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, FinalSegmentRope.asFinal("<.")));
        assertThrows(Throwable.class, () -> Term.wrap(DT_integer, FinalSegmentRope.asFinal(">..")));

        assertThrows(Throwable.class, () -> new FinalTerm(P_XSD, FinalSegmentRope.asFinal(">"), true));
    }

    static Stream<Arguments> testWrapInterns() {
        return Stream.of(
                arguments(null, FinalSegmentRope.asFinal("\"\""), EMPTY_STRING),
                arguments(P_XSD, FinalSegmentRope.asFinal("anyURI>"), XSD_ANYURI),
                arguments(P_XSD, FinalSegmentRope.asFinal(".anyURI>)").sub(1, 8), XSD_ANYURI),
                arguments(P_XSD, FinalSegmentRope.asFinal("unsignedInt>"), XSD_UNSIGNEDINT),
                arguments(P_RDF, FinalSegmentRope.asFinal("type>"), RDF_TYPE),
                arguments(P_RDF, FinalSegmentRope.asFinal("\"type>)").sub(1, 6), RDF_TYPE),
                arguments(P_RDF, FinalSegmentRope.asFinal("Property>"), RDF_PROPERTY),
                arguments(null, FinalSegmentRope.asFinal("\"1\""), Term.wrap(null, FinalSegmentRope.asFinal("\"1\""))),
                arguments(FinalSegmentRope.EMPTY, FinalSegmentRope.asFinal("\"aZ\""), Term.wrap(null, FinalSegmentRope.asFinal("\"aZ\"")))
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
        var iri = asFinal("<http://www.example.org/ns#>");
        SegmentRope sh = SHARED_ROPES.internPrefixOf(iri, 0, iri.len);
        assertNotNull(sh);

        assertSame(CLOSE_IRI, requireNonNull(wrap(sh, FinalSegmentRope.asFinal(">")).local()));
        Term one = wrap(sh, FinalSegmentRope.asFinal("1>"));
        Term ab = wrap(sh, FinalSegmentRope.asFinal("ab>"));
        assertNotNull(one);
        assertNotNull(ab);
        assertSame(one.local(), requireNonNull(wrap(sh, FinalSegmentRope.asFinal("1>")).local()));
        assertSame(ab.local(), requireNonNull(wrap(sh, FinalSegmentRope.asFinal("ab>")).local()));
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
        FinalTerm termFromTmp;
        try (var tmp = PooledMutableRope.get().append(utf8)) {
            termFromTmp = valueOf(tmp);
            assertEquals(in, termFromTmp.toString());
        }
        assertEquals(in, termFromTmp.toString()); // must remain valid

        var padd = RopeFactory.make(3 + utf8.length).add("'").add('"').add(utf8).add('>').take();
        Term wrapped = valueOf(padd, 2, 2+utf8.length);
        assertEquals(in, wrapped.toString());
        assertEquals(in, valueOf(in).toString());
    }

    @Test
    void testVar() {
        for (Term t : List.of(valueOf("?x"), valueOf("$x"))) {
            assertEquals(Type.VAR, t.type());
            assertEquals("x", requireNonNull(t.name()).toString());
        }
        for (Term t : List.of(valueOf(asFinal(", ?1"), 2, 4),
                valueOf(asFinal("_:$1"), 2, 4))) {
            assertEquals(Type.VAR, t.type());
            assertEquals("1", requireNonNull(t.name()).toString());
        }
        for (Term t : List.of(valueOf(asFinal(",?test123"), 1, 9),
                valueOf(asFinal("$test123")))) {
            assertEquals(Type.VAR, t.type());
            assertEquals("test123", requireNonNull(t.name()).toString());
        }

        for (Term t : List.of(valueOf("$test"), valueOf(asFinal("$test")),
                              valueOf(new FinalSegmentRope(ByteBuffer.wrap(",$test".getBytes(UTF_8))), 1, 6))) {
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
        TwoSegmentRope lex = new TwoSegmentRope();
        valueOf(asFinal(in)).escapedLexical(lex);
        assertEquals(ex, requireNonNull(lex).toString());

        valueOf(in).escapedLexical(lex);
        assertEquals(ex, requireNonNull(lex).toString());

        var padded = asFinal("\"" + in + "\"");
        valueOf(padded, 1, 1+in.length()).escapedLexical(lex);
        assertEquals(ex, requireNonNull(lex).toString());

    }

    private static PrefixAssigner CUSTOM_PREFIX_ASSIGNER;

    static {
        var customMap = RopeArrayMap.create().takeOwnership(TermTest.class);
        customMap.put(FinalSegmentRope.asFinal("<http://example.org/"), FinalSegmentRope.EMPTY);
        customMap.put(FinalSegmentRope.asFinal("<http://xmlns.com/foaf/0.1/"), Rope.asRope("foaf"));
        var orphan = customMap.releaseOwnership(TermTest.class);
        CUSTOM_PREFIX_ASSIGNER = PrefixAssigner.create(orphan).takeOwnership(TermTest.class);
    }

    @AfterAll static void afterAll() {
        CUSTOM_PREFIX_ASSIGNER = CUSTOM_PREFIX_ASSIGNER.recycle(TermTest.class);
    }

    static Stream<Arguments> testToSparql() {
        SegmentRope foaf = SHARED_ROPES.internPrefixOf(asFinal("<http://xmlns.com/foaf/0.1/>"), 0, 28);
        assertNotNull(foaf);

        return Stream.of(
                arguments(NOP, FinalSegmentRope.EMPTY, "_:bn", "_:bn"),
                arguments(NOP, FinalSegmentRope.EMPTY, "\"bob\"", "\"bob\""),
                arguments(NOP, FinalSegmentRope.EMPTY, "\"bob\"@en", "\"bob\"@en"),
                arguments(NOP, FinalSegmentRope.EMPTY, "\"bob\"@en-US", "\"bob\"@en-US"),
                arguments(NOP, FinalSegmentRope.EMPTY, "\"\\\"\"", "\"\\\"\""),
                arguments(NOP, FinalSegmentRope.EMPTY, "<>", "<>"),
                arguments(NOP, FinalSegmentRope.EMPTY, "<rel>", "<rel>"),
                arguments(NOP, FinalSegmentRope.EMPTY, "<http://example.org/>", "<http://example.org/>"),

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
                arguments(CUSTOM_PREFIX_ASSIGNER, foaf, "knows>", "foaf:knows")
        );
    }

    @ParameterizedTest @MethodSource
    void testToSparql(PrefixAssigner assigner, SegmentRope sh, String local, String expected) {
        for (String padLeft : List.of("", "<", "\"<")) {
            for (String padRight : List.of("", "\"", ">", ">\"")) {
                var ctx = "padLeft=" + padLeft + ", padRight=" + padRight
                        + ", sh=" + sh + ", local=" + local;
                int off = padLeft.length(), len = local.getBytes(UTF_8).length;
                try (var actual = PooledMutableRope.get()) {
                    byte[] u8 = (padLeft + local + padRight).getBytes(UTF_8);
                    MemorySegment seg = MemorySegment.ofArray(u8);
                    Term.toSparql(actual, assigner, sh, seg, u8, off, len,
                            sh.len > 0 && sh.get(0) == '"');
                    assertEquals(expected, actual.toString(), ctx);
                }
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
        try (var dest = PooledMutableRope.get()) {
            dest.append('@');
            int n = term.unescapedLexical(dest);
            assertEquals(dest.len, n + 1);
            assertEquals("@" + ex, dest.toString());
        }
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
                arguments("\"1.000\"^^xsd:float", "\"1.0\"^^xsd:double", 0)
//                arguments("\"52.5167\"^^xsd:float", "\"52.5166666666\"^^xsd:float", 0),
//                arguments("\"52.5167\"^^xsd:float", "\"52.51666666\"^^xsd:double", 0),
//                arguments("\"52.5167\"^^xsd:float", "\"52.51666\"^^xsd:decimal", 1),
//                arguments("\"52.5167\"^^xsd:float", "\"52.51666666\"^^xsd:decimal", 0),
//                arguments("\"52.5167\"^^xsd:float", "\"52.5166\"^^xsd:float", 1),
//                arguments("\"52.5167\"^^xsd:float", "\"52.5168\"^^xsd:float", -1),
//                arguments("\"52.5167\"^^xsd:float", "\"52.51655\"^^xsd:float", 1),
//                arguments("\"52.5167\"^^xsd:double", "\"52.51688\"^^xsd:float", -1),
//                arguments("\"52.5167\"^^xsd:float", "\"52.516666666666666\"^^xsd:float", 0),
//                arguments("\"52.3167\"^^xsd:float", "\"52.31666666666667\"^^xsd:float", 0),
//                arguments("\"12.4833\"^^xsd:double", "\"12.483333333333333\"^^xsd:double", 0),
//                arguments("\"12.4833\"^^xsd:float", "\"12.483333333333333\"^^xsd:float", 0),
//                arguments("\"12.4833\"^^xsd:double", "\"12.483333333333333\"^^xsd:float", 0),
//                arguments("\"23.7167\"^^xsd:float", "\"23.716666666666665\"^^xsd:float", 0),
//                arguments("\"23.7167\"^^xsd:float", "\"23.716666666666664\"^^xsd:float", 0),
//                arguments("\"23.7167\"^^xsd:float", "\"23.716666666666668\"^^xsd:float", 0),
//                arguments("\"-0.116667\"^^xsd:double", "\"-0.11666666666666667\"^^xsd:double", 0),
//                arguments("\"39.9167\"^^xsd:float", "\"39.916666666666664\"^^xsd:float", 0)

        );
    }

    @ParameterizedTest @MethodSource
    void testTolerantNumericComparison(String lStr, String rStr, int expected) {
        Term l = Objects.requireNonNull(Term.termList(lStr).getFirst());
        Term r = Objects.requireNonNull(Term.termList(rStr).getFirst());
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