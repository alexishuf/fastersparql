package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;

public class InvalidSparqlResultsException extends FSServerException {
    public InvalidSparqlResultsException(String msg) {super(null, msg);}
    public InvalidSparqlResultsException(SparqlEndpoint ep, String msg) {super(ep, msg);}
    public InvalidSparqlResultsException(Throwable cause) {
        super(null, cause.getMessage(), cause);
    }
}
