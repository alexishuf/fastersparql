package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.List;

@SuppressWarnings("unused")
public class RDFMediaTypes {
    public static final MediaType TRIG = new MediaType("application", "trig");
    public static final MediaType TTL = new MediaType("text", "turtle");
    public static final MediaType NT = new MediaType("application", "n-triples");
    public static final MediaType N3 = new MediaType("text", "n3");
    public static final MediaType JSONLD = new MediaType("application", "ld+json");
    public static final MediaType RDFXML = new MediaType("application", "rdf+xml");

    /**
     * List of media types to include in an Accept headers by default.
     * <p>
     * {@link RDFMediaTypes#N3} is exclude as N3 includes node types not covered by the
     * RDF Abstract Syntax and thus nearly no parsers are able to parse it. If a server
     * answers with {@code text/n3}, contents usually are in {@code application/n-triples} or
     * {@code text/turtle}, but could be in actual {@code text/n3}
     */
    public static final List<MediaType> DEFAULT_ACCEPTS = List.of(TTL, NT, TRIG, JSONLD, RDFXML);
}
