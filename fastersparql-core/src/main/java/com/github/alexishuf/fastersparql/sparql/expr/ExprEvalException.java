package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;

public class ExprEvalException extends SparqlClientException {
    public ExprEvalException(String message) {
        super(message);
    }
}
