package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.Results.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class CsvParserTest  extends ResultsParserTest {

    public static Stream<Arguments> test() {
        return Stream.of(
                // ask queries
                /*  1 */arguments("", negativeResult()),
                /*  2 */arguments("\r\n", negativeResult()),
                /*  3 */arguments("\r\n", negativeResult()),
                /*  4 */arguments("\r\n\r\n", positiveResult()),
                // parse vars (empty results)
                /*  5 */arguments("x\r\n", results(Vars.of("?x"))),
                /*  6 */arguments("x,y\r\n", results("?x", "?y")),
                /*  7 */arguments("\"x\",\"y\"\r\n", results("?x", "?y")),
                /*  8 */arguments("\"x & b\",\"y\"\r\n", results().error(InvalidSparqlResultsException.class)),
                /*  9 */arguments("\" x \",\"y\"\r\n", results("?x", "?y")),
                /* 10 */arguments("\",\",\"y\"\r\n", results().error(InvalidSparqlResultsException.class)),
                // single-column, single-row
                /* 11 */arguments("x\r\nbob\r\n", results("?x", "\"bob\"")),
                /* 12 */arguments("x\r\n23\r\n", results("?x", "\"23\"")),
                /* 13 */arguments("x\r\nhttp://example.org/Alice\r\n", results("?x", "<http://example.org/Alice>")),
                /* 14 */arguments("x\r\n\"http://example.org/Alice\"\r\n", results("?x", "<http://example.org/Alice>")),
                /* 15 */arguments("x\r\n_:b0\r\n", results("?x", "_:b0")),
                // multi-column
                /* 16 */arguments("x,y\r\n23,bob\r\n", results("?x", "?y", "\"23\"", "\"bob\"")),
                /* 17 */arguments("x,y\r\n23,bob\r\n",     results("?x", "?y", "\"23\"", "\"bob\"")),
                /* 18 */arguments("x,y\r\n_:,http://example.org/Alice\r\n",     results("?x", "?y", "_:", "<http://example.org/Alice>")),
                // multi-row and multi-column
                /* 19 */arguments("x,y\r\n23,bob\r\n\"a,\"\"b\"\"\",http://example.org/Alice\r\n",
                                  results("?x", "?y",
                                          "\"23\"", "\"bob\"",
                                          "\"a,\\\"b\\\"\"", "<http://example.org/Alice>")),
                /* 20 */arguments("x,y\r\n\"Line1\r\nLine2\",23\r\n_:,a thing\r\n",
                                  results("?x", "?y",
                                          "\"Line1\\r\\nLine2\"", "\"23\"",
                                          "_:", "\"a thing\"")),
                // Unescaped "
                /* 21 */arguments("x\r\n\"an \"\"a\".\"\r\n", results("?x").error(InvalidSparqlResultsException.class)),
                //unterminated "
                /* 22 */arguments("x\r\n\"asd\r\n", results("?x").error(InvalidSparqlResultsException.class)),
                /* 23 */arguments("x\r\n\"\r\n", results("?x").error(InvalidSparqlResultsException.class)),
                /* 24 */arguments("\"x\r\n", results("?x").error(InvalidSparqlResultsException.class)),
                // extra column
                /* 25 */arguments("x\r\n1,2\r\n", results("?x").error(InvalidSparqlResultsException.class)),
                // extra column, second line
                /* 26 */arguments("x\r\n11\r\n21,22\r\n", results("?x", "\"11\"").error(InvalidSparqlResultsException.class)),
                // positive ask with explicit boolean
                /* 27 */arguments("_askResult\r\ntrue\r\n", positiveResult()),
                // positive ask with explicit boolean, quoted
                /* 28 */arguments("_askResult\r\n\"true\"", positiveResult()),
                // negative ask with explicit boolean
                /* 29 */arguments("_askResult\r\nfalse\r\n", negativeResult()),
                // negative ask with explicit boolean, quoted
                /* 30 */arguments("_askResult\r\n\"false\"", negativeResult()),
                // negative ask with no rows
                /* 31 */arguments("_askResult\r\n", negativeResult())
        );
    }

    @ParameterizedTest @MethodSource
    void test(String in, Results expected) throws Exception {
        doTest(new SVParser.CsvFactory(), expected, FinalSegmentRope.asFinal(in));
    }
}
