package com.github.alexishuf.fastersparql.client.exceptions;

public class InvalidSparqlQuery extends SparqlClientInvalidArgument {
    protected String query;

    public InvalidSparqlQuery(CharSequence query) {
        super("Invalid SPARQL query: "+query);
        this.query = query.toString();
    }

    public String query() { return query; }
}
