package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;

public class InvalidSparqlResultsException extends SparqlClientException {
    public InvalidSparqlResultsException(String message) {
        super(message);
    }
}
