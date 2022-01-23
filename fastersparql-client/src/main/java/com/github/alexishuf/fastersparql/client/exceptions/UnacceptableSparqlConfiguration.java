package com.github.alexishuf.fastersparql.client.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

@Getter
public class UnacceptableSparqlConfiguration extends SparqlClientInvalidArgument {
    private @MonotonicNonNull String uri;
    private final SparqlConfiguration offer, request;

    public UnacceptableSparqlConfiguration(SparqlConfiguration offer,
                                           SparqlConfiguration request) {
        super(String.format("Requested config %s is not satisfied by config %s", request, offer));
        this.offer = offer;
        this.request = request;
    }

    public UnacceptableSparqlConfiguration(String uri, SparqlConfiguration offer,
                                           SparqlConfiguration request) {
        this(uri, offer, request, String.format("Requested config %s is not satisfied by config " +
                "%s for SPARQL endpoint at %s", request, offer, uri));
    }

    public UnacceptableSparqlConfiguration(String uri, SparqlConfiguration offer,
                                           SparqlConfiguration request, String message) {
        super(message);
        this.uri = uri;
        this.offer = offer;
        this.request = request;
    }

    public UnacceptableSparqlConfiguration(SparqlEndpoint endpoint,
                                           SparqlConfiguration request) {
        this(endpoint.uri(), endpoint.configuration(), request);
    }

    public UnacceptableSparqlConfiguration uri(String uri) {
        this.uri = uri;
        return this;
    }
}
