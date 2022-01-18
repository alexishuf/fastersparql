package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RDFMediaTypes {
    public static final MediaType TRIG = new MediaType("application", "trig");
    public static final MediaType TTL = new MediaType("text", "turtle");
    public static final MediaType NT = new MediaType("application", "n-triples");
    public static final MediaType N3 = new MediaType("text", "n3");
    public static final MediaType JSONLD = new MediaType("application", "ld+json");
    public static final MediaType RDFXML = new MediaType("application", "rdf+xml");

    public static final List<MediaType> ALL = Collections.unmodifiableList(Arrays.asList(
            TTL, NT, TRIG, N3, JSONLD, RDFXML));
}
