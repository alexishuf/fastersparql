package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.util.UriUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NettySparqlClientTest {
    public static Stream<Arguments> testWriteParams() {
        String sparql = "SELECT ?x WHERE { ?s a <http://schema.org/Person>.}";
        String escSparql = UriUtils.escapeQueryParam(sparql);
        Map<String, List<String>> map = new HashMap<>();
        map.put("x", singletonList("a%20b"));
        map.put("y", singletonList("7"));
        return Stream.of(
        /*  1 */arguments("", '\0', sparql, emptyMap(), "query="+escSparql),
        /*  2 */arguments("", '?', sparql, emptyMap(), "?query="+escSparql),
        /*  3 */arguments("", '&', sparql, emptyMap(), "&query="+escSparql),
        /*  4 */arguments("/sparql", '?', sparql, emptyMap(), "/sparql?query="+escSparql),
        /*  5 */arguments("/sparql?user=bob", '&', sparql, emptyMap(), "/sparql?user=bob&query="+escSparql),
        /*  6 */arguments("", '\0', null, map, "x=a%20b&y=7"),
        /*  7 */arguments("", '?', null, map, "?x=a%20b&y=7"),
        /*  8 */arguments("", '&', null, map, "&x=a%20b&y=7"),
        /*  9 */arguments("/sparql", '?', null, map, "/sparql?x=a%20b&y=7"),
        /* 10 */arguments("/sparql?user=bob", '&', null, map, "/sparql?user=bob&x=a%20b&y=7"),
        /* 11 */arguments("/sparql?user=bob", '&', sparql, map, "/sparql?user=bob&query="+escSparql+"&x=a%20b&y=7"),
        /* 12 */arguments("/sparql", '?', sparql, map, "/sparql?query="+escSparql+"&x=a%20b&y=7")
        );
    }

    @ParameterizedTest @MethodSource
    void testWriteParams(String prefix, char firstSeparator, String sparql,
                         Map<String, List<String>> map, String expected) {
        StringBuilder mutablePrefix = new StringBuilder(prefix);
        StringBuilder mutableSparql = sparql == null ? null : new StringBuilder(sparql);
        CharSequence actual = NettySparqlClient.writeParams(mutablePrefix, firstSeparator,
                mutableSparql, map);
        assertEquals(expected, actual.toString());
        assertEquals(prefix, mutablePrefix.toString());
        assertEquals(sparql, mutableSparql == null ? null : mutableSparql.toString());
    }

}