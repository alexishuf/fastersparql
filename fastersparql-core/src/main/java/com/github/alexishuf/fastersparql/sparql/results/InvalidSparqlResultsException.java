package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;

public class InvalidSparqlResultsException extends FSServerException {
    public InvalidSparqlResultsException(String message) { super(null, message); }
    public InvalidSparqlResultsException(SparqlEndpoint endpoint, String message) { super(endpoint, message); }
    public InvalidSparqlResultsException(Throwable cause) {
        super(null, cause.getMessage(), cause);
    }
}
