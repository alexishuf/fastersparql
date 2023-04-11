package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.RopeDict;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.model.rope.RopeDict.DT_string;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.*;
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

                arguments(Term.valueOf("_:a"), "_:a."),
                arguments(Term.valueOf("_:a"), "_:a,"),
                arguments(Term.valueOf("_:a"), "_:a;"),
                arguments(Term.valueOf("_:a"), "_:a "),
                arguments(Term.valueOf("_:a"), "_:a\t"),
                arguments(Term.valueOf("_:a"), "_:a\n"),
                arguments(Term.valueOf("_:a"), "_:a\r"),
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

                arguments(Term.plainString("a"), "\"a\"^^xsd:string\n"),
                arguments(Term.plainString("a"), "\"\"\"a\"\"\"^^xsd:string\t"),
                arguments(Term.plainString("a"), "'a'^^xsd:string,"),
                arguments(Term.plainString("a"), "'''a'''^^xsd:string."),

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
                arguments(Term.EMPTY_STRING, "\"\"^^xsd:string."),
                arguments(Term.EMPTY_STRING, "\"\"\"\"\"\"^^xsd:string;"),
                arguments(Term.EMPTY_STRING, "''^^xsd:string\n"),
                arguments(Term.EMPTY_STRING, "''''''^^xsd:string "),

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

                arguments(Term.typed("23", RopeDict.DT_INT), "\"23\"^^xsd:int\t"),
                arguments(Term.typed("23", RopeDict.DT_INT), "\"\"\"23\"\"\"^^xsd:int "),
                arguments(Term.typed("23", RopeDict.DT_INT), "'23'^^xsd:int\n"),
                arguments(Term.typed("23", RopeDict.DT_INT), "'''23'''^^xsd:int."),

                //boolean literals (Turtle)
                arguments(Term.TRUE, "true."),
                arguments(Term.FALSE, "false,"),
                arguments(Term.TRUE, "true\r"),
                arguments(Term.FALSE, "false\n"),
                arguments(Term.TRUE, "true;"),
                arguments(Term.FALSE, "false\t"),

                //number literals (Turtle)
                arguments(Term.typed("23", RopeDict.DT_integer), "23,"),
                arguments(Term.typed("-23", RopeDict.DT_integer), "-23;"),
                arguments(Term.typed("23.5", RopeDict.DT_decimal), "23.5\t"),
                arguments(Term.typed("-23.5", RopeDict.DT_decimal), "-23.5\n"),
                arguments(Term.typed("23e+2", RopeDict.DT_DOUBLE), "23e+2 "),
                arguments(Term.typed("23E-2", RopeDict.DT_DOUBLE), "23E-2\r"),

                //number literals followed by '.' (Turtle)
                arguments(Term.typed("23", RopeDict.DT_integer), "23."),
                arguments(Term.typed("-23", RopeDict.DT_integer), "-23."),
                arguments(Term.typed("23.5", RopeDict.DT_decimal), "23.5."),
                arguments(Term.typed("-23.5", RopeDict.DT_decimal), "-23.5."),
                arguments(Term.typed("23e+2", RopeDict.DT_DOUBLE), "23e+2."),
                arguments(Term.typed("23E-2", RopeDict.DT_DOUBLE), "23E-2."),

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
                arguments(Term.XSD_STRING, "xsd:string."),
                arguments(Term.XSD_STRING, "xsd:string,"),
                arguments(Term.XSD_STRING, "xsd:string\n"),
                arguments(Term.XSD_STRING, "xsd:string\t"),
                arguments(Term.RDF_TYPE, "rdf:type."),
                arguments(Term.RDF_TYPE, "rdf:type,"),
                arguments(Term.RDF_TYPE, "rdf:type\n"),
                arguments(Term.RDF_TYPE, "rdf:type\t"),
                arguments(Term.RDF_TYPE, "a "),
                arguments(Term.RDF_TYPE, "a,"),
                arguments(Term.RDF_TYPE, "a."),
                arguments(Term.RDF_TYPE, "a;"),
                arguments(Term.iri("http://example.org/ns#predicate"), "ex:predicate."),
                arguments(Term.iri("http://example.org/ns#predicate"), "ex:predicate,"),
                arguments(Term.iri("http://example.org/ns#predicate"), "ex:predicate;"),
                arguments(Term.iri("http://example.org/ns#predicate"), "ex:predicate "),
                arguments(Term.iri("http://example.org/ns#predicate"), "ex:predicate\t"),
                arguments(Term.iri("http://example.org/ns#predicate"), "ex:predicate\n"),
                arguments(Term.iri("http://example.org/ns#predicate-1"), "ex:predicate-1."),
                arguments(Term.iri("http://example.org/ns#predicate-1"), "ex:predicate-1;"),
                arguments(Term.iri("http://example.org/ns#predicate-1"), "ex:predicate-1,"),
                arguments(Term.iri("http://example.org/ns#predicate-1"), "ex:predicate-1\t"),
                arguments(Term.iri("http://example.org/ns#predicate-1"), "ex:predicate-1\n"),
                arguments(Term.iri("http://example.org/ns#p"), "ex:p."),
                arguments(Term.iri("http://example.org/ns#p"), "ex:p,"),
                arguments(Term.iri("http://example.org/ns#p"), "ex:p;"),
                arguments(Term.iri("http://example.org/ns#p"), "ex:p\n"),
                arguments(Term.iri("http://example.org/ns#p"), "ex:p\t"),
                arguments(Term.iri("http://example.org/p"), ":p."),
                arguments(Term.iri("http://example.org/p"), ":p;"),
                arguments(Term.iri("http://example.org/p"), ":p,"),
                arguments(Term.iri("http://example.org/p"), ":p\t"),

                //prefixed datatypes
                arguments(Term.typed("5", DT_string), "\"5\"^^xsd:string."),
                arguments(Term.typed("23", RopeDict.DT_INT), "\"23\"^^xsd:int,"),
                arguments(Term.typed("<p>", RopeDict.DT_HTML), "\"<p>\"^^rdf:HTML\n"),

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

    private void assertParse(Term expected, String in, int start, int inEnd, boolean eager) {
        int len = in.length();
        int expectedTermEnd = switch (inEnd > start ? in.charAt(inEnd-1) : 0) {
            case ',', '.', ';', '\t', '\n', '\r', ' ' -> inEnd-1;
            default                                   -> inEnd;
        };
        TermParser parser = new TermParser();
        if (eager)
            assertSame(parser, parser.eager());
        parser.prefixMap.add(Rope.of("ex"), Term.iri("http://example.org/ns#"));
        parser.prefixMap.add(Rope.of(""), Term.iri("http://example.org/"));
        Rope inRope = Rope.of(in);
        TermParser.Result result = parser.parse(inRope, start, len);
        assertEquals(expected != ERROR, result.isValid());

        if (expected == ERROR) {
            assertThrows(InvalidTermException.class, parser::asTerm);
        } else {
            assertEquals(expectedTermEnd, parser.termEnd());
            assertEquals(expected, parser.asTerm());
            assertEquals(expected.toString(), parser.asTerm().toString());
            assertEquals(expectedTermEnd, parser.termEnd(), "termEnd changed by as*() methods");

            assertEquals(expected.flaggedDictId, parser.flaggedId());
            switch (parser.result()) {
                case NT, VAR -> {
                    int begin = parser.localBegin(), end = parser.localEnd();
                    assertSame(inRope, parser.localBuf());
                    assertEquals(new ByteRope(expected.local),
                                 requireNonNull(parser.localBuf()).sub(begin, end));
                }
            }

            int fId = parser.flaggedId();
            assertEquals(expected.flaggedDictId, fId);
            int localBegin = parser.localBegin(), localEnd = parser.localEnd();
            Rope local = parser.localBuf().sub(localBegin, localEnd);
            Rope reassembled = fId < 0 ? Rope.of(local, RopeDict.getTolerant(fId))
                                       : Rope.of(RopeDict.getTolerant(fId), local);
            assertEquals(expected.toString(), reassembled.toString());
        }
    }

    void testParse(Term expected, String ntOrTtl, boolean eager) {
        int len = ntOrTtl.length();
        assertParse(expected, ntOrTtl, 0, len, eager);
        // parser must ignore everything before start
        String traps = "?$_:<>\"\"''\\";
        for (char first : traps.toCharArray()) {
            String in = traps+first+ntOrTtl;
            assertParse(expected, in, traps.length()+1, in.length(), eager);
        }
        // parser must detect by itself when the term ends
        for (char stop : ".,;){ \n".toCharArray()) {
            assertParse(expected, ntOrTtl+stop, 0, len, eager);
            assertParse(expected, stop+ntOrTtl+stop, 1, len +1, eager);
        }
        if (!eager && ntOrTtl.endsWith(".")) // test eager parse
            testParse(expected, ntOrTtl.substring(0, ntOrTtl.length()-1), true);
    }

    @ParameterizedTest @MethodSource
    void testParse(Term expected, String ntOrTtl) {
        testParse(expected, ntOrTtl, false);
    }
}
