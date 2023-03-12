package com.github.alexishuf.fastersparql.exceptions;

public class InvalidSparqlQueryType extends InvalidSparqlQuery {
    public InvalidSparqlQueryType(CharSequence query) {
        super("Unexpected query type: "+query.toString().replace("\n", "\\n"));
    }
}
