package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.util.UriUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.FSProperties.CLIENT_MAX_QUERY_GET;
import static com.github.alexishuf.fastersparql.client.model.SparqlConfiguration.builder;
import static com.github.alexishuf.fastersparql.client.model.SparqlMethod.*;
import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.firstLine;
import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.formString;
import static com.github.alexishuf.fastersparql.model.RDFMediaTypes.*;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
class SparqlClientHelpersTest {
    @AfterEach
    void tearDown() {
        System.clearProperty(CLIENT_MAX_QUERY_GET);
    }

    static Stream<Arguments> testResultsAcceptString() {
        String tsv = "text/tab-separated-values";
        String csv = "text/csv";
        String json = "application/sparql-results+json";
        String xml = "application/sparql-results+xml";
        return Stream.of(
                arguments(singletonList(TSV), tsv),
                arguments(singletonList(JSON), json),
                arguments(asList(TSV, JSON), tsv+", "+json+"; q=0.9"),
                arguments(asList(JSON, TSV), json+", "+tsv+"; q=0.9"),
                arguments(asList(JSON, TSV, XML), json+", "+tsv+"; q=0.9, "+xml+"; q=0.8"),
                arguments(asList(JSON, TSV, XML, CSV), json+", "+tsv+"; q=0.9, "+xml+"; q=0.8, "+csv+"; q=0.7")
        );
    }

    @ParameterizedTest @MethodSource
    void testResultsAcceptString(List<SparqlResultFormat> list, String expected) {
        assertEquals(expected, SparqlClientHelpers.resultsAcceptString(list));
    }

    static Stream<Arguments> testRdfAcceptString() {
        String ttl = "text/turtle";
        String nt = "application/n-triples";
        String xml = "application/rdf+xml";
        return Stream.of(
                arguments(singletonList(TTL), ttl),
                arguments(singletonList(NT), nt),
                arguments(asList(TTL, NT), ttl+", "+nt+"; q=0.9"),
                arguments(asList(TTL, NT, RDFXML), ttl+", "+nt+"; q=0.9, "+xml+"; q=0.8")
        );
    }

    @ParameterizedTest @MethodSource
    void testRdfAcceptString(List<MediaType> types, String expected) {
        assertEquals(expected, SparqlClientHelpers.rdfAcceptString(types));
    }

    static Stream<Arguments> testFirstLine() {
        String ex = "http://example.org";
        SparqlConfiguration get = builder().method(GET).method(POST).build();
        SparqlConfiguration post = builder().method(POST).method(GET).build();
        SparqlConfiguration form = builder().method(FORM).method(GET).build();

        SparqlConfiguration getSingleParamConfig = builder().method(GET)
                .param(Rope.of("x"), Rope.ropeList("23")).build();
        SparqlConfiguration postSingleParamConfig = builder().method(POST)
                .param(Rope.of("x"), Rope.ropeList("23")).build();
        SparqlConfiguration formSingleParamConfig = builder().method(FORM)
                .param(Rope.of("x"), Rope.ropeList("23")).build();

        SparqlConfiguration getMultiParamConfig = builder().method(GET)
                .param(Rope.of("x"), Rope.ropeList("2", "3"))
                .param(Rope.of("par"), Rope.ropeList("test")).build();
        SparqlConfiguration postMultiParamConfig = builder().method(POST)
                .param(Rope.of("x"), Rope.ropeList("2", "3"))
                .param(Rope.of("par"), Rope.ropeList("test")).build();
        SparqlConfiguration formMultiParamConfig = builder().method(FORM)
                .param(Rope.of("x"), Rope.ropeList("2", "3"))
                .param(Rope.of("par"), Rope.ropeList("test")).build();

        String sparql = "SELECT ?x WHERE { ?x a <http://schema.org/Person>}";
        String escaped = UriUtils.escapeQueryParam(sparql).toString();
        return Stream.of(
                arguments(ex+"/sparql", form, sparql, "/sparql"),
                arguments(ex+"/sparql", post, sparql, "/sparql"),
                arguments(ex+"/sparql", get, sparql, "/sparql?query="+escaped),

                arguments(ex+"/", form, sparql, "/"),
                arguments(ex+"/", post, sparql, "/"),
                arguments(ex+"/", get, sparql, "/?query="+escaped),

                arguments(ex+"/sparql?opt=val", form, sparql, "/sparql?opt=val"),
                arguments(ex+"/sparql?opt=val", post, sparql, "/sparql?opt=val"),
                arguments(ex+"/sparql?opt=val", get, sparql, "/sparql?opt=val&query="+escaped),

                arguments(ex+"/sparql", formSingleParamConfig, sparql, "/sparql"),
                arguments(ex+"/sparql", postSingleParamConfig, sparql, "/sparql?x=23"),
                arguments(ex+"/sparql", getSingleParamConfig, sparql, "/sparql?query="+escaped+"&x=23"),

                arguments(ex+"/sparql", formMultiParamConfig, sparql, "/sparql"),
                arguments(ex+"/sparql", postMultiParamConfig, sparql, "/sparql?x=2&x=3&par=test"),
                arguments(ex+"/sparql", getMultiParamConfig, sparql, "/sparql?query="+escaped+"&x=2&x=3&par=test")
        );
    }

    @ParameterizedTest @MethodSource
    void testFirstLine(String endpoint, SparqlConfiguration config, String sparql,
                       String expected) {
        SparqlEndpoint parsed = SparqlEndpoint.parse(endpoint);
        assertEquals(expected, firstLine(parsed, config, new ByteRope(sparql)));
    }

    static Stream<Arguments> testFormString() {
        String sparql = "SELECT ?x WHERE { ?x a <http://schema.org/Person>}";
        String escaped = UriUtils.escapeQueryParam(sparql).toString();
        Map<String, List<String>> singleParam = new HashMap<>();
        singleParam.put("x", List.of("1"));
        Map<String, List<String>> singleList = new HashMap<>();
        singleList.put("x", List.of("11", "test"));
        Map<String, List<String>> multiParams = new HashMap<>();
        multiParams.put("x", List.of("11", "test"));
        multiParams.put("y", List.of("23"));
        return Stream.of(
                arguments(sparql, emptyMap(), "query="+escaped),
                arguments(sparql, singleParam, "query="+escaped+"&x=1"),
                arguments(sparql, singleList, "query="+escaped+"&x=11&x=test"),
                arguments(sparql, multiParams, "query="+escaped+"&x=11&x=test&y=23")
        );
    }

    @ParameterizedTest @MethodSource
    void testFormString(String sparqlStr, Map<String, List<String>> paramsStr, String expectedStr) {
        Rope sparql = Rope.of(sparqlStr);
        Map<Rope, List<Rope>> params = new HashMap<>();
        for (var e : paramsStr.entrySet())
            params.put(Rope.of(e.getKey()), Rope.ropeList(e.getValue()));
        Rope expected = Rope.of(expectedStr);
        assertEquals(expected, formString(sparql, params));
    }
}