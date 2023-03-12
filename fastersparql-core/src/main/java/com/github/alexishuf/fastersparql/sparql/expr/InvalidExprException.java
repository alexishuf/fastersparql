package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;

public class InvalidExprException extends InvalidSparqlException {
    public InvalidExprException(Object input, int pos, String reason) {
        super("Bad SPARQL expression \""
                +input.toString().replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
                +"\". "+reason+" at position "+pos);
    }

    public InvalidExprException(String message) {
        super(message);
    }
}
