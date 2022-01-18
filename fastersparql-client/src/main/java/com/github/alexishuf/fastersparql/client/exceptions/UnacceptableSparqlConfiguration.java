package com.github.alexishuf.fastersparql.client.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class UnacceptableSparqlConfiguration extends SparqlClientInvalidArgument {
    String uri;
    SparqlConfiguration offer, request;

    public UnacceptableSparqlConfiguration(String uri, SparqlConfiguration offer,
                                           SparqlConfiguration request) {
        super(String.format("Requested config %s is not satisfied by config %s for SPARQL " +
                            "endpoint at %s", request, offer, uri));
        this.uri = uri;
        this.offer = offer;
        this.request = request;
    }

    public UnacceptableSparqlConfiguration(SparqlEndpoint endpoint,
                                           SparqlConfiguration request) {
        this(endpoint.uri(), endpoint.configuration(), request);
    }
}
