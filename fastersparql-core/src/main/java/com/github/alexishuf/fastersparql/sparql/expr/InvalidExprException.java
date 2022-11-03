package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;

public class InvalidExprException extends InvalidSparqlException {
    public InvalidExprException(String input, int pos, String reason) {
        super("Bad SPARQL expression \""+input+"\". "+reason+" at position "+pos);
    }

    public InvalidExprException(String message) {
        super(message);
    }
}
