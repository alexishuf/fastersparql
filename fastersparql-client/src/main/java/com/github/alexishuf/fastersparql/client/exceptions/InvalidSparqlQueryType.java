package com.github.alexishuf.fastersparql.client.exceptions;

import java.util.List;

public class InvalidSparqlQueryType extends InvalidSparqlQuery {
    private final List<String> expectedTypes;

    public InvalidSparqlQueryType(CharSequence query, List<String> expectedTypes) {
        super(query);
        this.expectedTypes = expectedTypes;
    }

    @SuppressWarnings("unused")
    public List<String> expectedTypes() { return expectedTypes; }

    @Override public String getMessage() {
        return "Expected a "+String.join("/", expectedTypes)+" query, got "+query;
    }
}
