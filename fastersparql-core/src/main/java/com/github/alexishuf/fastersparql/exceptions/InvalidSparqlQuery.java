package com.github.alexishuf.fastersparql.exceptions;

public class InvalidSparqlQuery extends FSInvalidArgument {
    public final String query;

    public InvalidSparqlQuery(CharSequence query) {
        super("Invalid SPARQL query: "+query);
        this.query = query.toString();
    }
}
