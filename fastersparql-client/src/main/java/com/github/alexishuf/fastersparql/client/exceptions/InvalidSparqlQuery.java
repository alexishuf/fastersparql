package com.github.alexishuf.fastersparql.client.exceptions;

import lombok.Getter;
import lombok.experimental.Accessors;

@Getter @Accessors(fluent = true)
public class InvalidSparqlQuery extends SparqlClientInvalidArgument {
    String query;

    public InvalidSparqlQuery(CharSequence query) {
        super("Invalid SPARQL query: "+query);
        this.query = query.toString();
    }
}
