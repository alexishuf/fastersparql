package com.github.alexishuf.fastersparql.sparql.expr;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.sparql.RDFTypes.*;
import static com.github.alexishuf.fastersparql.sparql.expr.TermParser.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TermParserTest {
    private static final Term ERROR = new Term.IRI("<throw%20error>");

    static Stream<Arguments> testParse() {
        return Stream.of(
                arguments(ERROR, null),

                arguments(ERROR, "a"),
                arguments(ERROR, "/"),
                arguments(ERROR, "@"),
                arguments(ERROR, "^^xsd:"),
                arguments(ERROR, "(<a>)"),
                arguments(ERROR, "http://example.org"),

                arguments(new Term.IRI("<a>"), "<a>"),
                arguments(ERROR, "<a"),

                arguments(new Term.BNode("_:a"), "_:a"),
                arguments(ERROR, "_2"),
                arguments(ERROR, "_"),
                arguments(ERROR, "["),
                arguments(ERROR, "[ :prop 2]"),

                arguments(new Term.Lit("a", string, null), "\"a\""),
                arguments(new Term.Lit("a", string, null), "\"\"\"a\"\"\""),
                arguments(new Term.Lit("a", string, null), "'a'"),
                arguments(new Term.Lit("a", string, null), "'''a'''"),

                arguments(new Term.Lit("a\\nb", string, null), "\"a\\nb\""),
                arguments(new Term.Lit("a\\nb", string, null), "\"\"\"a\\nb\"\"\""),
                arguments(new Term.Lit("a\\nb", string, null), "'a\\nb'"),
                arguments(new Term.Lit("a\\nb", string, null), "'''a\\nb'''"),

                arguments(new Term.Lit("a\\\"b", string, null), "\"a\\\"b\""),
                arguments(new Term.Lit("a\"b", string, null), "\"\"\"a\"b\"\"\""),
                arguments(new Term.Lit("a'b", string, null), "\"a'b\""),
                arguments(new Term.Lit("a'b", string, null), "\"\"\"a'b\"\"\""),
                arguments(new Term.Lit("a\"b", string, null), "'a\"b'"),
                arguments(new Term.Lit("a\"b", string, null), "'''a\"b'''"),
                arguments(new Term.Lit("a\\'b", string, null), "'a\\'b'"),
                arguments(new Term.Lit("a'b", string, null), "'''a'b'''"),

                arguments(new Term.Lit("a", string, null), "\"a\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(new Term.Lit("a", string, null), "\"\"\"a\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(new Term.Lit("a", string, null), "'a'^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(new Term.Lit("a", string, null), "'''a'''^^<http://www.w3.org/2001/XMLSchema#string>"),

                arguments(new Term.Lit("a", string, null), "\"a\"^^xsd:string"),
                arguments(new Term.Lit("a", string, null), "\"\"\"a\"\"\"^^xsd:string"),
                arguments(new Term.Lit("a", string, null), "'a'^^xsd:string"),
                arguments(new Term.Lit("a", string, null), "'''a'''^^xsd:string"),

                arguments(new Term.Lit("a", langString, "en"), "\"a\"@en"),
                arguments(new Term.Lit("a", langString, "en"), "\"\"\"a\"\"\"@en"),
                arguments(new Term.Lit("a", langString, "en"), "'a'@en"),
                arguments(new Term.Lit("a", langString, "en"), "'''a'''@en"),

                //parse empty strings
                arguments(new Term.Lit("", string, null), "\"\""),
                arguments(new Term.Lit("", string, null), "\"\"\"\"\"\""),
                arguments(new Term.Lit("", string, null), "''"),
                arguments(new Term.Lit("", string, null), "''''''"),

                //parse empty typed strings
                arguments(new Term.Lit("", string, null), "\"\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(new Term.Lit("", string, null), "\"\"\"\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(new Term.Lit("", string, null), "''^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(new Term.Lit("", string, null), "''''''^^<http://www.w3.org/2001/XMLSchema#string>"),

                //parse empty xsd-typed strings
                arguments(new Term.Lit("", string, null), "\"\"^^xsd:string"),
                arguments(new Term.Lit("", string, null), "\"\"\"\"\"\"^^xsd:string"),
                arguments(new Term.Lit("", string, null), "''^^xsd:string"),
                arguments(new Term.Lit("", string, null), "''''''^^xsd:string"),

                //parse empty lang-tagged strings
                arguments(new Term.Lit("", langString, "en-US"), "\"\"@en-US"),
                arguments(new Term.Lit("", langString, "en"), "\"\"\"\"\"\"@en"),
                arguments(new Term.Lit("", langString, "en"), "''@en"),
                arguments(new Term.Lit("", langString, "en-US"), "''''''@en-US"),

                //parse quote strings
                arguments(new Term.Lit("\\\"", langString, "en-US"), "\"\\\"\"@en-US"),
                arguments(new Term.Lit("\\\"", langString, "en"), "\"\"\"\\\"\"\"\"@en"),
                arguments(new Term.Lit("\\'", langString, "en"), "'\\''@en"),
                arguments(new Term.Lit("\\'", langString, "en-US"), "'''\\''''@en-US"),
                arguments(new Term.Lit("'", langString, "en-US"), "'''''''@en-US"),

                arguments(new Term.Lit("23", INT, null), "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(new Term.Lit("23", INT, null), "\"\"\"23\"\"\"^^<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(new Term.Lit("23", INT, null), "'23'^^<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(new Term.Lit("23", INT, null), "'''23'''^^<http://www.w3.org/2001/XMLSchema#int>"),

                arguments(new Term.Lit("23", INT, null), "\"23\"^^xsd:int"),
                arguments(new Term.Lit("23", INT, null), "\"\"\"23\"\"\"^^xsd:int"),
                arguments(new Term.Lit("23", INT, null), "'23'^^xsd:int"),
                arguments(new Term.Lit("23", INT, null), "'''23'''^^xsd:int"),

                arguments(Term.Lit.TRUE, "true"),
                arguments(Term.Lit.FALSE, "false"),

                arguments(new Term.Lit("23", integer, null), "23"),
                arguments(new Term.Lit("-23", integer, null), "-23"),
                arguments(new Term.Lit("23.5", decimal, null), "23.5"),
                arguments(new Term.Lit("-23.5", decimal, null), "-23.5"),
                arguments(new Term.Lit("23e+2", DOUBLE, null), "23e+2"),
                arguments(new Term.Lit("23E-2", DOUBLE, null), "23E-2"),

                arguments(new Term.Var("?x"), "?x"),
                arguments(new Term.Var("?xX"), "?xX"),
                arguments(new Term.Var("?1"), "?1"),
                arguments(new Term.Var("$1_2"), "$1_2"),
                arguments(new Term.Var("$x"), "$x"),

                arguments(ERROR, "\"\"\"a\""),
                arguments(ERROR, "'''a''"),
                arguments(ERROR, "'''''"),
                arguments(ERROR, "'\""),
                arguments(ERROR, "\"'"),
                arguments(ERROR, "'a'@"),
                arguments(ERROR, "'a'^"),
                arguments(ERROR, "'a'^^"),
                arguments(ERROR, "'a'^^<"),
                arguments(ERROR, "'a'^^<a")
        );
    }

    private void assertParse(Term expected, String in, int start, int end) {
        if (expected.equals(ERROR)) {
            assertThrows(InvalidTermException.class, () -> parse(in, start, null));
            return;
        }
        int[] acEnd = {0};
        assertEquals(expected, parse(in, start, null));
        assertEquals(expected, parse(in, start, acEnd));
        assertEquals(end, acEnd[0]);
    }

    @ParameterizedTest @MethodSource
    void testParse(Term expected, String ntOrTtl) {
        int len = ntOrTtl == null ? 0 : ntOrTtl.length();
        assertParse(expected, ntOrTtl, 0, len);
        // parser must ignore everything before start
        String traps = "?$_:<>\"\"''\\";
        for (char first : traps.toCharArray()) {
            String in = traps+first+ntOrTtl;
            assertParse(expected, in, traps.length()+1, in.length());
        }
        // parser must detect by itself when the term ends
        for (char stop : ".,;){ \n#".toCharArray()) {
            assertParse(expected, ntOrTtl+stop, 0, len);
            assertParse(expected, stop+ntOrTtl+stop, 1, len +1);
        }
    }
}
