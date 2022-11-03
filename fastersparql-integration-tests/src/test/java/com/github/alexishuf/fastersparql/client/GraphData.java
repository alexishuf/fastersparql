package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.graph.GraphFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("FieldMayBeFinal")
public class GraphData {
    private static final String PREFIX, TTL_PREFIX;

    private Class<? extends Throwable> error;
    private SparqlQuery sparql;
    private String expectedTTL;

    public GraphData(String sparql, String expectedTTL) {
        this.sparql = new SparqlQuery(PREFIX+sparql);
        this.expectedTTL = TTL_PREFIX+expectedTTL;
    }

    public static GraphData graph(String sparql, String expectedTTL) {
        return new GraphData(sparql, expectedTTL);
    }

    public void assertExpected(SparqlClient<?,?,?> client) {
        Graph<?> graph = client.queryGraph(sparql);
        var expectedMediaType = client.endpoint().configuration().rdfAccepts().get(0);
        var actualMediaType = graph.mediaType();
        Charset charset = actualMediaType.charset(UTF_8);
        var actual = new StringBuilder();
        try (var it = graph.it()) {
            it.forEachRemaining(f -> decodeFragment(f, actual, charset));
        } catch (Throwable t) {
            if (error == null)
                fail(t);
            else if (!error.isAssignableFrom(t.getClass()))
                fail("Expected "+error.getSimpleName()+", got "+t);
        }
        assertTrue(expectedMediaType.accepts(actualMediaType),
                   "expected "+expectedMediaType+", got "+actualMediaType);
        String ttl = this.expectedTTL;
        if (RDFMediaTypes.JSONLD.accepts(actualMediaType))
            ttl = expectedTTL.replaceAll("@en-US", "@en-us");
        org.apache.jena.graph.Graph acGraph = parse(actual.toString(), actualMediaType);
        org.apache.jena.graph.Graph exGraph = parse(ttl, RDFMediaTypes.TTL);
        assertEquals(exGraph.size(), acGraph.size());
        assertTrue(exGraph.isIsomorphicWith(acGraph));
    }

    /* --- --- --- helper methods --- --- --- */

    static {
        TTL_PREFIX = """
                @prefix :     <http://example.org/>.
                @prefix xsd:  <http://www.w3.org/2001/XMLSchema#>.
                @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
                @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
                @prefix owl:  <http://www.w3.org/2002/07/owl#>.
                @prefix foaf: <http://xmlns.com/foaf/0.1/>.
                """;
        PREFIX = TTL_PREFIX.replaceAll("@prefix", "PREFIX").replaceAll(">\\.\n", ">\n");
    }

    void decodeFragment(Object fragment, StringBuilder destination, Charset charset) {
        assertNotNull(fragment);
        switch (fragment) {
            case CharSequence cs -> destination.append(cs);
            case char[] chars    -> destination.append(new String(chars));
            case byte[] bytes    -> destination.append(charset.decode(ByteBuffer.wrap(bytes)));
            case default         -> fail("Unexpected fragment type: " + fragment.getClass());
        }
    }

    org.apache.jena.graph.Graph parse(String data, MediaType mediaType) {
        if (RDFMediaTypes.JSONLD.accepts(mediaType))
            data = data.replaceAll("\"@language\" : \"en-us\"", "\"@language\" : \"en-US\"");
        org.apache.jena.graph.Graph jGraph = GraphFactory.createDefaultGraph();
        ByteArrayInputStream bis = new ByteArrayInputStream(data.getBytes(UTF_8));
        Lang lang = RDFLanguages.contentTypeToLang(mediaType.withoutParams().toString());
        RDFDataMgr.read(jGraph, bis, lang);
        return jGraph;
    }
}
