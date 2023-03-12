package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.RopeDict.DT_string;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TermParserTest {
    private static final Term ERROR = Term.iri("<throw%20error>");

    static Stream<Arguments> testParse() {
        return Stream.of(
                arguments(ERROR, "/"),
                arguments(ERROR, "@"),
                arguments(ERROR, "^^xsd:"),
                arguments(ERROR, "(<a>)"),
                arguments(ERROR, "http://example.org"),

                arguments(Term.iri("<a>"), "<a>"),
                arguments(ERROR, "<a"),

                arguments(Term.valueOf("_:a"), "_:a"),
                arguments(ERROR, "_2"),
                arguments(ERROR, "_"),
                arguments(ERROR, "["),
                arguments(ERROR, "[ :prop 2]"),

                arguments(Term.plainString("a"), "\"a\""),
                arguments(Term.plainString("a"), "\"\"\"a\"\"\""),
                arguments(Term.plainString("a"), "'a'"),
                arguments(Term.plainString("a"), "'''a'''"),

                arguments(Term.plainString("a\\nb"), "\"a\\nb\""),
                arguments(Term.plainString("a\\nb"), "\"\"\"a\\nb\"\"\""),
                arguments(Term.plainString("a\\nb"), "'a\\nb'"),
                arguments(Term.plainString("a\\nb"), "'''a\\nb'''"),

                arguments(Term.plainString("a\\\"b"), "\"a\\\"b\""),
                arguments(Term.plainString("a\\\"b"), "\"\"\"a\"b\"\"\""),
                arguments(Term.plainString("a'b"), "\"a'b\""),
                arguments(Term.plainString("a'b"), "\"\"\"a'b\"\"\""),
                arguments(Term.plainString("a\\\"b"), "'a\"b'"),
                arguments(Term.plainString("a\\\"b"), "'''a\"b'''"),
                arguments(Term.plainString("a\\'b"), "'a\\'b'"),
                arguments(Term.plainString("a'b"), "'''a'b'''"),

                arguments(Term.plainString("a"), "\"a\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(Term.plainString("a"), "\"\"\"a\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(Term.plainString("a"), "'a'^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(Term.plainString("a"), "'''a'''^^<http://www.w3.org/2001/XMLSchema#string>"),

                arguments(Term.plainString("a"), "\"a\"^^xsd:string"),
                arguments(Term.plainString("a"), "\"\"\"a\"\"\"^^xsd:string"),
                arguments(Term.plainString("a"), "'a'^^xsd:string"),
                arguments(Term.plainString("a"), "'''a'''^^xsd:string"),

                arguments(Term.lang("a","en"), "\"a\"@en"),
                arguments(Term.lang("a","en"), "\"\"\"a\"\"\"@en"),
                arguments(Term.lang("a","en"), "'a'@en"),
                arguments(Term.lang("a","en"), "'''a'''@en"),

                //parse empty strings
                arguments(Term.EMPTY_STRING, "\"\""),
                arguments(Term.EMPTY_STRING, "\"\"\"\"\"\""),
                arguments(Term.EMPTY_STRING, "''"),
                arguments(Term.EMPTY_STRING, "''''''"),

                //parse empty typed strings
                arguments(Term.EMPTY_STRING, "\"\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(Term.EMPTY_STRING, "\"\"\"\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(Term.EMPTY_STRING, "''^^<http://www.w3.org/2001/XMLSchema#string>"),
                arguments(Term.EMPTY_STRING, "''''''^^<http://www.w3.org/2001/XMLSchema#string>"),

                //parse empty xsd-typed strings
                arguments(Term.EMPTY_STRING, "\"\"^^xsd:string"),
                arguments(Term.EMPTY_STRING, "\"\"\"\"\"\"^^xsd:string"),
                arguments(Term.EMPTY_STRING, "''^^xsd:string"),
                arguments(Term.EMPTY_STRING, "''''''^^xsd:string"),

                //parse empty lang-tagged strings
                arguments(Term.lang("", "en-US"), "\"\"@en-US"),
                arguments(Term.lang("", "en"), "\"\"\"\"\"\"@en"),
                arguments(Term.lang("", "en"), "''@en"),
                arguments(Term.lang("", "en-US"), "''''''@en-US"),

                //parse quote strings
                arguments(Term.lang("\\\"", "en-US"), "\"\\\"\"@en-US"),
                arguments(Term.lang("\\\"", "en"), "\"\"\"\\\"\"\"\"@en"),
                arguments(Term.lang("\\'", "en"), "'\\''@en"),
                arguments(Term.lang("\\'", "en-US"), "'''\\''''@en-US"),
                arguments(Term.lang("'", "en-US"), "'''''''@en-US"),

                arguments(Term.typed("23", RopeDict.DT_INT), "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(Term.typed("23", RopeDict.DT_INT), "\"\"\"23\"\"\"^^<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(Term.typed("23", RopeDict.DT_INT), "'23'^^<http://www.w3.org/2001/XMLSchema#int>"),
                arguments(Term.typed("23", RopeDict.DT_INT), "'''23'''^^<http://www.w3.org/2001/XMLSchema#int>"),

                arguments(Term.typed("23", RopeDict.DT_INT), "\"23\"^^xsd:int"),
                arguments(Term.typed("23", RopeDict.DT_INT), "\"\"\"23\"\"\"^^xsd:int"),
                arguments(Term.typed("23", RopeDict.DT_INT), "'23'^^xsd:int"),
                arguments(Term.typed("23", RopeDict.DT_INT), "'''23'''^^xsd:int"),

                //boolean literals (Turtle)
                arguments(Term.TRUE, "true"),
                arguments(Term.FALSE, "false"),

                //number literals (Turtle)
                arguments(Term.typed("23", RopeDict.DT_integer), "23"),
                arguments(Term.typed("-23", RopeDict.DT_integer), "-23"),
                arguments(Term.typed("23.5", RopeDict.DT_decimal), "23.5"),
                arguments(Term.typed("-23.5", RopeDict.DT_decimal), "-23.5"),
                arguments(Term.typed("23e+2", RopeDict.DT_DOUBLE), "23e+2"),
                arguments(Term.typed("23E-2", RopeDict.DT_DOUBLE), "23E-2"),

                // vars
                arguments(Term.valueOf("?x"), "?x"),
                arguments(Term.valueOf("?xX"), "?xX"),
                arguments(Term.valueOf("?1"), "?1"),
                arguments(Term.valueOf("$1_2"), "$1_2"),
                arguments(Term.valueOf("$x"), "$x"),

                //NT IRIs
                arguments(Term.iri("http://ex.org/"), "<http://ex.org/>"),
                arguments(Term.iri("http://www.example.org/filler/Bob#me"), "<http://www.example.org/filler/Bob#me>"),
                arguments(Term.iri("23"), "<23>"),
                arguments(Term.iri("-23"), "<-23>"),
                arguments(Term.iri("1"), "<1>"),

                //prefixed IRIs
                arguments(Term.XSD_STRING, "xsd:string"),
                arguments(Term.RDF_TYPE, "rdf:type"),
                arguments(Term.RDF_TYPE, "a"),
                arguments(Term.iri("http://example.org/ns#predicate"), "ex:predicate"),
                arguments(Term.iri("http://example.org/ns#predicate-1"), "ex:predicate-1"),
                arguments(Term.iri("http://example.org/ns#p"), "ex:p"),
                arguments(Term.iri("http://example.org/p"), ":p"),

                //prefixed datatypes
                arguments(Term.typed("5", DT_string), "\"5\"^^xsd:string"),
                arguments(Term.typed("23", RopeDict.DT_INT), "\"23\"^^xsd:int"),
                arguments(Term.typed("<p>", RopeDict.DT_HTML), "\"<p>\"^^rdf:HTML"),

                //errors
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

    private void assertParse(Term expected, String in, int start, int expectedTermEnd) {
        int len = in.length();
        TermParser parser = new TermParser();
        parser.prefixMap.add(Rope.of("ex"), Term.iri("http://example.org/ns#"));
        parser.prefixMap.add(Rope.of(""), Term.iri("http://example.org/"));
        TermParser.Result result = parser.parse(Rope.of(in), start, len);
        assertEquals(expected != ERROR, result.isValid());

        if (expected == ERROR) {
            assertThrows(InvalidTermException.class, parser::asTerm);
        } else {
            assertEquals(expectedTermEnd, parser.termEnd());
            assertEquals(expected, parser.asTerm());
            assertEquals(expected.toString(), parser.asTerm().toString());
            assertEquals(expectedTermEnd, parser.termEnd(), "termEnd changed by as*() methods");
        }
    }

    @ParameterizedTest @MethodSource
    void testParse(Term expected, String ntOrTtl) {
        int len = ntOrTtl.length();
        assertParse(expected, ntOrTtl, 0, len);
        // parser must ignore everything before start
        String traps = "?$_:<>\"\"''\\";
        for (char first : traps.toCharArray()) {
            String in = traps+first+ntOrTtl;
            assertParse(expected, in, traps.length()+1, in.length());
        }
        // parser must detect by itself when the term ends
        for (char stop : ".,;){ \n".toCharArray()) {
            assertParse(expected, ntOrTtl+stop, 0, len);
            assertParse(expected, stop+ntOrTtl+stop, 1, len +1);
        }
    }
}
