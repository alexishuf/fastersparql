package com.github.alexishuf.fastersparql.client.util.sparql;

import com.github.alexishuf.fastersparql.sparql.RDFTypes;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TermTest {
    static Stream<Arguments> testAsBool() {
        return Stream.of(
                arguments(true, Term.Lit.TRUE),
                arguments(false, Term.Lit.FALSE),
                arguments(false, new Term.Lit("", string, null)),
                arguments(false, new Term.Lit("", RDFTypes.langString, "en")),
                arguments(true, new Term.Lit("0", string, null)),
                arguments(true, new Term.Lit("false", string, null)),
                arguments(true, new Term.Lit("a", RDFTypes.langString, "en")),
                arguments(false, new Term.Lit("0", RDFTypes.INT, null)),
                arguments(false, new Term.Lit("0", RDFTypes.FLOAT, null)),
                arguments(false, new Term.Lit("0", RDFTypes.DOUBLE, null)),
                arguments(false, new Term.Lit("0", RDFTypes.LONG, null)),
                arguments(false, new Term.Lit("0", RDFTypes.SHORT, null)),
                arguments(false, new Term.Lit("0", RDFTypes.BYTE, null)),
                arguments(false, new Term.Lit("0", RDFTypes.integer, null)),
                arguments(false, new Term.Lit("0", RDFTypes.decimal, null)),
                arguments(null, new Term.IRI("<http://example.org/Alice>")),
                arguments(null, new Term.BNode("_:asd"))
        );
    }

    @ParameterizedTest @MethodSource
    void testAsBool(Boolean expected, Term term) {
        if (expected == null)
            assertThrows(IllegalArgumentException.class, term::asBool);
        else
            assertEquals(expected, term.asBool());
    }

    static Stream<Arguments> testAsNumber() {
        return Stream.of(
                arguments(null, Term.Lit.TRUE),
                arguments(null, Term.Lit.FALSE),
                arguments(null, new Term.Lit("", string, null)),
                arguments(null, new Term.Lit("0", string, null)),
                arguments(null, new Term.Lit("23", string, null)),
                arguments(null, new Term.Lit("-23.4e2", string, null)),
                arguments(0, new Term.Lit("0", INT, null)),
                arguments(23, new Term.Lit("23", INT, null)),
                arguments(-23, new Term.Lit("-23", INT, null)),
                arguments(23, new Term.Lit("+23", INT, null)),
                arguments(23L, new Term.Lit("23", LONG, null)),
                arguments(23.0, new Term.Lit("23", DOUBLE, null)),
                arguments(23.2, new Term.Lit("23.2", DOUBLE, null)),
                arguments(-23e2, new Term.Lit("-23E2", DOUBLE, null)),
                arguments(23.0f, new Term.Lit("23", FLOAT, null)),
                arguments(Short.valueOf("23"), new Term.Lit("23", SHORT, null)),
                arguments(Byte.valueOf("23"), new Term.Lit("23", BYTE, null))
        );
    }

    @ParameterizedTest @MethodSource
    void testAsNumber(Number expected, Term.Lit lit) {
        assertEquals(expected, lit.asNumber());
    }

    static Stream<Arguments> testCompare() {
        Term.Lit i23 = new Term.Lit("23", INT, null);
        Term.Lit l23 = new Term.Lit("23", LONG, null);
        Term.Lit s23 = new Term.Lit("23", SHORT, null);
        Term.Lit b23 = new Term.Lit("23", BYTE, null);
        Term.Lit d23 = new Term.Lit("23.0", DOUBLE, null);
        Term.Lit f23 = new Term.Lit("23.0", FLOAT, null);
        Term.Lit D23 = new Term.Lit("23.0", decimal, null);
        Term.Lit I23 = new Term.Lit("23", integer, null);

        Term.Lit i24 = new Term.Lit("24", INT, null);
        Term.Lit l24 = new Term.Lit("24", LONG, null);
        Term.Lit s24 = new Term.Lit("24", SHORT, null);
        Term.Lit b24 = new Term.Lit("24", BYTE, null);
        Term.Lit d24 = new Term.Lit("24", DOUBLE, null);
        Term.Lit f24 = new Term.Lit("24", FLOAT, null);
        Term.Lit D24 = new Term.Lit("24", decimal, null);
        Term.Lit I24 = new Term.Lit("24", integer, null);
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

    @ParameterizedTest @MethodSource
    void testCompare(int expected, Term.Lit left, Term.Lit right) {
        assertEquals(expected, left.compareTo(right));
        assertEquals(-1 * expected, right.compareTo(left));
        if (expected == 0)
            assertEquals(left, right);
    }

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(false, new Term.IRI("<a>"), new Term.IRI("<b>")),
                arguments(true, new Term.IRI("<a>"), new Term.IRI("<a>")),
                arguments(false, new Term.BNode("_:a"), new Term.BNode("_:b")),
                arguments(true, new Term.BNode("_:a"), new Term.BNode("_:a")),
                arguments(false, new Term.Lit("a", string, null),
                                 new Term.Lit("b", string, null)),
                arguments(true, new Term.Lit("a", string, null),
                                new Term.Lit("a", string, null)),
                arguments(true, new Term.Lit("a", langString, "en"),
                                new Term.Lit("a", langString, "en")),
                arguments(false, new Term.Lit("a", langString, "en"),
                                 new Term.Lit("a", langString, "pt"))
        );
    }

    @ParameterizedTest @MethodSource
    void testEquals(boolean expected, Term l, Term r) {
        assertEquals(expected, l.equals(r));
        assertEquals(expected, r.equals(l));
    }

    static Stream<Arguments> testNT() {
        return Stream.of(
                arguments(new Term.IRI("<a>"), "<a>"),
                arguments(new Term.IRI("<http://example.org:8080/a?x=[1]^^>"), "<http://example.org:8080/a?x=[1]^^>"),
                arguments(new Term.BNode("_:a"), "_:a"),
                arguments(new Term.BNode("_:a23-"), "_:a23-"),
                arguments(new Term.Var("?x"), "?x"),
                arguments(new Term.Var("?2_x"), "?2_x"),
                arguments(new Term.Var("$2_x"), "$2_x"),
                arguments(new Term.Lit("a", string, null), "\"a\""),
                arguments(new Term.Lit("23e2", DOUBLE, null), "\"23e2\"^^<http://www.w3.org/2001/XMLSchema#double>"),
                arguments(new Term.Lit("+23.0", decimal, null), "\"+23.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"),
                arguments(new Term.Lit("1 a ?x", string, null), "\"1 a ?x\""),
                arguments(new Term.Lit("\\\"", string, null), "\"\\\"\"")
        );
    }

    @ParameterizedTest @MethodSource
    void testNT(Term term, String expected) {
        assertEquals(expected, term.nt());
        assertEquals(expected, term.toString());
    }
}