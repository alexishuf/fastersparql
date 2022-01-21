package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.model.SparqlResultFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.model.RDFMediaTypes.*;
import static com.github.alexishuf.fastersparql.client.model.SparqlConfiguration.builder;
import static com.github.alexishuf.fastersparql.client.model.SparqlMethod.*;
import static com.github.alexishuf.fastersparql.client.model.SparqlResultFormat.*;
import static com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties.CLIENT_MAX_QUERY_GET;
import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    static Stream<Arguments> testEffectiveConfig() {
        SparqlEndpoint offer = SparqlEndpoint.parse("get,form,tsv,json@http://example.org/sparql");
        SparqlEndpoint noGetOffer = SparqlEndpoint.parse("form,tsv,xml@http://example.org/sparql");
        SparqlEndpoint onlyGetOffer = SparqlEndpoint.parse("get,tsv,xml@http://example.org/sparql");
        SparqlEndpoint ttl = SparqlEndpoint.parse("ttl@http://example.org/sparql");
        return Stream.of(
                arguments(offer,
                          builder()
                                  .method(GET).method(POST)
                                  .resultsAccept(TSV).resultsAccept(XML)
                                  .header("X-ray", "123").build(),
                          900, -1,
                          builder()
                                  .method(GET)
                                  .resultsAccept(TSV)
                                  .header("x-ray", "123").build()
                ),
                // large query de-prioritizes get by default
                arguments(offer, builder().build(), 8192, -1,
                          builder()
                                .method(SparqlMethod.FORM).method(GET)
                                .resultsAccept(TSV).resultsAccept(JSON).build()),
                // large query de-prioritizes get
                arguments(offer, builder().build(), 1025, 1024,
                          builder()
                                .method(SparqlMethod.FORM).method(GET)
                                .resultsAccept(TSV).resultsAccept(JSON).build()),
                // large query uses get if no other method is allowed
                arguments(onlyGetOffer, builder().build(), 1025, 1024,
                          builder().method(GET)
                                .resultsAccept(TSV).resultsAccept(XML).build()),
                //unacceptable method
                arguments(noGetOffer, builder().method(GET).build(), 256, -1, null),
                //unacceptable result
                arguments(noGetOffer, builder().resultsAccept(CSV).build(), 256, -1, null),
                //unacceptable rdfResult
                arguments(ttl, builder().rdfAccept(TRIG).build(), 256, -1, null)
        );
    }

    @ParameterizedTest @MethodSource
    void testEffectiveConfig(SparqlEndpoint ep, SparqlConfiguration request, int queryLen,
                             int maxQueryByGet, SparqlConfiguration expected) {
        if (maxQueryByGet != -1)
            System.setProperty(CLIENT_MAX_QUERY_GET, String.valueOf(maxQueryByGet));
        if (expected == null) {
            assertThrows(UnacceptableSparqlConfiguration.class,
                         () -> effectiveConfig(ep, request, queryLen));
        } else {
            assertEquals(expected, effectiveConfig(ep, request, queryLen));
        }
    }

    static Stream<Arguments> testFirstLine() {
        String ex = "http://example.org";
        SparqlConfiguration get = builder().method(GET).method(POST).build();
        SparqlConfiguration post = builder().method(POST).method(GET).build();
        SparqlConfiguration form = builder().method(FORM).method(GET).build();

        SparqlConfiguration getSingleParamConfig = builder().method(GET)
                .param("x", singletonList("23")).build();
        SparqlConfiguration postSingleParamConfig = builder().method(POST)
                .param("x", singletonList("23")).build();
        SparqlConfiguration formSingleParamConfig = builder().method(FORM)
                .param("x", singletonList("23")).build();

        SparqlConfiguration getMultiParamConfig = builder().method(GET)
                .param("x", asList("2", "3"))
                .param("par", singletonList("test")).build();
        SparqlConfiguration postMultiParamConfig = builder().method(POST)
                .param("x", asList("2", "3"))
                .param("par", singletonList("test")).build();
        SparqlConfiguration formMultiParamConfig = builder().method(FORM)
                .param("x", asList("2", "3"))
                .param("par", singletonList("test")).build();

        String sparql = "SELECT ?x WHERE { ?x a <http://schema.org/Person>}";
        String escaped = UriUtils.escapeQueryParam(sparql);
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
        assertEquals(expected, firstLine(parsed, config, sparql).toString());
    }

    static Stream<Arguments> testFormString() {
        String sparql = "SELECT ?x WHERE { ?x a <http://schema.org/Person>}";
        String escaped = UriUtils.escapeQueryParam(sparql);
        Map<String, List<String>> singleParam = new HashMap<>();
        singleParam.put("x", singletonList("1"));
        Map<String, List<String>> singleList = new HashMap<>();
        singleList.put("x", asList("11", "test"));
        Map<String, List<String>> multiParams = new HashMap<>();
        multiParams.put("x", asList("11", "test"));
        multiParams.put("y", singletonList("23"));
        return Stream.of(
                arguments(sparql, emptyMap(), "query="+escaped),
                arguments(sparql, singleParam, "query="+escaped+"&x=1"),
                arguments(sparql, singleList, "query="+escaped+"&x=11&x=test"),
                arguments(sparql, multiParams, "query="+escaped+"&x=11&x=test&y=23")
        );
    }

    @ParameterizedTest @MethodSource
    void testFormString(String sparql, Map<String, List<String>> params, String expected) {
        assertEquals(expected, formString(sparql, params).toString());
    }
}