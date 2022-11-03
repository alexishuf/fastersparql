package com.github.alexishuf.fastersparql.sparql;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;

public class InvalidSparqlException extends SparqlClientException {
    public InvalidSparqlException(String message) {
        super(message);
    }
}
