package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.util.Results;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.Results.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class JsonParserTest extends ResultsParserTest {

    public static Stream<Arguments> test() {
        return Stream.of(
                /*  1 */arguments("[]", results().error(InvalidSparqlResultsException.class)),
                /*  2 */arguments("{}", results().error(InvalidSparqlResultsException.class)),
                // non-empty response with unexpected properties
                /*  3 */arguments("{\"x\": false}", results().error(InvalidSparqlResultsException.class)),
                // empty vars, no results
                /*  4 */arguments("{\"head\": {\"vars\": []}}", negativeResult()),
                //empty vars, null results
                /*  5 */arguments("{\"head\": {\"vars\": []}, \"results\": null}", negativeResult()),
                //empty vars, null bindings
                /*  6 */arguments("{\"head\": {\"vars\": []}, \"results\": {\"bindings\": null}}", negativeResult()),
                //empty vars, empty bindings
                /*  7 */arguments("{\"head\": {\"vars\": []}, \"results\": {\"bindings\": []}}", negativeResult()),

                // single var, no bindings
                /*  8 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": []}}",
                                  results(Vars.of("x"))),
                // two vars, no bindings
                /*  9 */arguments("{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": []}}",
                                  results("?x", "?y")),

                // single var, single typed result
                /* 10 */arguments("""
                                  {
                                    "head": {
                                      "vars": [ "x" ]
                                    }
                                    "results" : {
                                      "bindings" : [
                                        {
                                          "x" : {
                                            "value": 23,
                                            "type": "literal",
                                            "datatype": "http://www.w3.org/2001/XMLSchema#integer"
                                          }
                                        }
                                      ]
                                    }
                                  }
                                  """,
                                 results("?x", "23")),
                // single var, single lang-tagged result
                /* 11 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\", \"xml:lang\": \"en\"}}" +
                                  "]}}",
                                  results("?x", "\"bob\"@en")),
                // single var, plain literal
                /* 12 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\"}}" +
                                  "]}}",
                                  results("?x", "\"alice\"")),
                // single var, three rows
                /* 13 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"23\", \"type\":\"literal\", \"datatype\": \"http://www.w3.org/2001/XMLSchema#int\"}}," +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\", \"xml:lang\": \"en_US\"}}," +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}}," +
                                  "]}}",
                                 results("?x",
                                         "\"23\"^^<http://www.w3.org/2001/XMLSchema#int>",
                                         "\"alice\"@en-US",
                                         "\"bob\"")),
                //single var extra var on binding
                /* 14 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}, " +
                                  "\"y\": {\"value\": \"wrong\", \"type\":\"literal\"}}," +
                                  "]}}",
                                  results("?x", "\"bob\"")),
                //single var extra var on 2nd binding
                /* 15 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\"}}," +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}, " +
                                  " \"y\": {\"value\": \"wrong\", \"type\":\"literal\"}}," +
                                  "]}}",
                                  results("?x", "\"alice\"", "\"bob\"")),
                //single var empty value object on 2nd
                /* 16 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"alice\", \"type\":\"literal\"}}," +
                                  "{\"x\": {}}," +
                                  "]}}",
                                  results("?x", "\"alice\"", null)),
                //single var empty binding on 2nd
                /* 17 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}}," +
                                  "{}" +
                                  "]}}",
                                  results("?x", "\"bob\"", null)),
                //single var empty binding
                /* 18 */arguments("{\"head\": {\"vars\": [\"x\"]}, \"results\": {\"bindings\": [{}]}}",
                                  results("?x", null)),

                //two vars two rows with unbound on first
                /* 19 */arguments("{\"head\": {\"vars\": [\"x\", \"y\"]}, \"results\": {\"bindings\": [" +
                                  "{\"x\": {\"value\": \"bob\", \"type\":\"literal\"}, \"y\": null}," +
                                  "{\"x\": {\"value\": \"charlie\", \"type\":\"literal\"}}," +
                                  "{\"x\": {\"type\":\"literal\", \"value\": \"alice\", \"xml:lang\": \"en\"},"+
                                  " \"y\": {\"type\":\"literal\", \"value\": \"charlie\"}}" +
                                  "]}}",
                                  results("?x", "?y",
                                          "\"bob\"", null,
                                          "\"charlie\"", null,
                                          "\"alice\"@en", "\"charlie\"")),

                //negative ask
                /* 20 */arguments("{\"head\": {}, \"boolean\": false}", negativeResult()),
                //positive ask
                /* 21 */arguments("{\"head\": {}, \"boolean\": true}", positiveResult()),
                //no-head negative ask
                /* 22 */arguments("{\"boolean\": false}", negativeResult()),
                //no-head positive ask
                /* 23 */arguments("{\"boolean\": true}", positiveResult()),
                //negative ask with links
                /* 24 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": false}",
                        negativeResult()),
                //positive ask with links
                /* 25 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": true}",
                        positiveResult()),

                //negative ask typed as string
                /* 26 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"false\"}",
                        negativeResult()),
                //positive ask typed as string
                /* 27 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"true\"}",
                        positiveResult()),
                //negative ask typed as camel string
                /* 28 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"False\"}",
                        negativeResult()),
                //positive ask typed as camel string
                /* 29 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"True\"}",
                        positiveResult()),
                //negative ask typed as number
                /* 30 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": 0}",
                        negativeResult()),
                //positive ask typed as number
                /* 31 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": 1}",
                        positiveResult()),
                //ask typed as unguessable string
                /* 32 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": \"x\"}",
                        results().error(InvalidSparqlResultsException.class)),
                //negative ask typed as null
                /* 33 */arguments("{\"head\": {\"link\": [\"http://example.com\"]}, \"boolean\": null}",
                        negativeResult()),
                //positive ask with empty vars list
                /* 34 */arguments("{\"head\": {\"vars\": []}, \"boolean\": true}", positiveResult()),
                //negative ask with empty vars list
                /* 35 */arguments("{\"head\": {\"vars\": []}, \"boolean\": false}", negativeResult())
        );
    }

    @ParameterizedTest @MethodSource
    void test(String in, Results expected) throws Exception {
        doTest(new JsonParser.JsonFactory(), expected, SegmentRope.of(in));
    }
}
