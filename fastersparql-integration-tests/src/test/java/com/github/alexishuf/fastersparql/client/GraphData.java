package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.RDFMediaTypes;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.reactive.IterableAdapter;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.graph.GraphFactory;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.*;

@Data @Accessors(fluent = true, chain = true)
public class GraphData {
    private static final String PREFIX, TTL_PREFIX;

    private SparqlMethod method;
    private MediaType accepts;
    private Class<? extends Throwable> error;
    private String sparql;
    private String expectedTTL;

    public GraphData(String sparql, String expectedTTL) {
        this.sparql = PREFIX+sparql;
        this.expectedTTL = TTL_PREFIX+expectedTTL;
    }

    public GraphData(GraphData other) {
        this.method = other.method;
        this.accepts = other.accepts;
        this.error = other.error;
        this.sparql = other.sparql;
        this.expectedTTL = other.expectedTTL;
    }

    public static GraphData graph(String sparql, String expectedTTL) {
        return new GraphData(sparql, expectedTTL);
    }

    public GraphData dup() {
        return new GraphData(this);
    }

    public SparqlConfiguration config() {
        SparqlConfiguration.SparqlConfigurationBuilder b = SparqlConfiguration.builder();
        if (method != null)
            b.clearMethods().method(method);
        if (accepts != null)
            b.clearRdfAccepts().rdfAccept(accepts);
        return b.build();
    }

    public void assertExpected(Graph<?> graph) {
        StringBuilder actual = new StringBuilder();
        IterableAdapter<?> adapter = new IterableAdapter<>(graph.publisher());
        for (Object o : adapter)
            decodeFragment(o, actual);
        MediaType actualMediaType = graph.mediaType().get();
        if (error != null) {
            fail("Expected error "+error.getSimpleName()+" got "+adapter.error());
            assertNull(actualMediaType);
        } else if (adapter.hasError()) {
            fail(adapter.error());
        }
        assertTrue(accepts.accepts(actualMediaType),
                   "expected "+accepts+", got "+actualMediaType);
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
        TTL_PREFIX = "@prefix :     <http://example.org/>.\n" +
                     "@prefix xsd:  <http://www.w3.org/2001/XMLSchema#>.\n" +
                     "@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>.\n" +
                     "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.\n" +
                     "@prefix owl:  <http://www.w3.org/2002/07/owl#>.\n" +
                     "@prefix foaf: <http://xmlns.com/foaf/0.1/>.\n";
        PREFIX = TTL_PREFIX.replaceAll("@prefix", "PREFIX").replaceAll(">\\.\n", ">\n");
    }

    void decodeFragment(Object fragment, StringBuilder destination) {
        assertNotNull(fragment);
        if (fragment instanceof CharSequence) {
            destination.append((CharSequence) fragment);
        } else if (fragment instanceof char[]) {
            destination.append(new String((char[])fragment));
        } else  if (fragment instanceof byte[]) {
            ByteBuffer input = ByteBuffer.wrap((byte[]) fragment);
            CharBuffer decoded = requireNonNull(accepts.charset(UTF_8)).decode(input);
            destination.append(decoded);
        } else {
            fail("Unexpected fragment type: "+fragment.getClass());
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
