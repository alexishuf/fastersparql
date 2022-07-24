package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;

public class InvalidSparqlResultsException extends SparqlClientServerException {

    public InvalidSparqlResultsException(String message) {
        this(null, message);
    }

    public InvalidSparqlResultsException(SparqlEndpoint endpoint, String message) {
        super(endpoint, message);
    }
}
