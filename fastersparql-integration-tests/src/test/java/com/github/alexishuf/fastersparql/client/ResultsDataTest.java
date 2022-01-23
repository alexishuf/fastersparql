package com.github.alexishuf.fastersparql.client;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultsDataTest {
    @ParameterizedTest @ValueSource(strings = {
            "   | ",   //empty in, empty out
            "$  | $",  // nothing after $
            ".$ | .$", // nothing after $
            "http://example.org/ | http://example.org/", //no $
            "?x                  | ?x",                  //no $
            "_:b0                | _:b0",                //no $
            "\".\"               | \".\"",                //no $
            "$: | http://example.org/", // input is just var
            "$foaf: | http://xmlns.com/foaf/0.1/", // input is just var
            "$xx   | $xx",   // no match
            "$xx:  | $xx:",  // no match
            "$x:   | $x:",   // no match
            "$e x: | $e x:", // no match
            "$.:   | $.:",   // no match
            //replace two vars:
            "$:Bob is a $foaf:Person | http://example.org/Bob is a http://xmlns.com/foaf/0.1/Person",
            //replace two vars (now with <>'s:
            "<$:Bob> is a <$foaf:Person> | <http://example.org/Bob> is a <http://xmlns.com/foaf/0.1/Person>"
    })
    void testExpandVars(String dataString) {
        String[] data = dataString.split(" *\\| *");
        if (data.length == 0)
            data = new String[] {"", ""};
        ResultsData results = new ResultsData("SELECT ?x WHERE {}", data[0]);
        assertEquals(data[1], results.expected().get(0).get(0));
    }

    @Test
    void testExpandNull() {
        ResultsData single = new ResultsData("SELECT ?x WHERE { ?x a :X}", "$null");
        assertEquals(singletonList(singletonList(null)), single.expected());

        ResultsData second = new ResultsData("SELECT ?x ?y WHERE { ?x a ?y}", "\"1\"", "$null");
        assertEquals(singletonList(asList("\"1\"", null)), second.expected());

        ResultsData firstSecond = new ResultsData("SELECT ?x WHERE { ?x a :X}", "\"1\"", "$null");
        assertEquals(asList(singletonList("\"1\""), singletonList(null)), firstSecond.expected());
    }

    @Test
    void testAsk() {
        ResultsData positive = new ResultsData("ASK {?x foaf:knows :Bob}", true);
        ResultsData negative = new ResultsData("ASK {?x foaf:knows :Bob}", false);
        assertEquals(1, positive.expected().size());
        assertTrue(positive.expected().get(0).isEmpty());
        assertTrue(negative.expected().isEmpty());
        assertEquals(Collections.emptyList(), positive.vars());
        assertEquals(Collections.emptyList(), negative.vars());
    }
}