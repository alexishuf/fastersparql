package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class UnacceptableSparqlConfiguration extends FSInvalidArgument {
    private @MonotonicNonNull String uri;
    private final SparqlConfiguration offer, request;

    public UnacceptableSparqlConfiguration(SparqlConfiguration offer,
                                           SparqlConfiguration request) {
        super(String.format("Requested config %s is not satisfied by config %s", request, offer));
        this.offer = offer;
        this.request = request;
    }

    public UnacceptableSparqlConfiguration(String uri, SparqlConfiguration offer,
                                           SparqlConfiguration request, String message) {
        super(message);
        this.uri = uri;
        this.offer = offer;
        this.request = request;
    }

    public UnacceptableSparqlConfiguration uri(String uri) {
        this.uri = uri;
        return this;
    }

    public String uri() { return uri; }
    @SuppressWarnings("unused") public SparqlConfiguration offer() { return offer; }
    public SparqlConfiguration request() { return request; }
}
