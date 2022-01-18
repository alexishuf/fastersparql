package com.github.alexishuf.fastersparql.client.exceptions;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.List;

@Getter @Accessors(fluent = true)
public class InvalidSparqlQueryType extends InvalidSparqlQuery {
    private final List<String> expectedTypes;

    public InvalidSparqlQueryType(CharSequence query, List<String> expectedTypes) {
        super(query);
        this.expectedTypes = expectedTypes;
    }

    @Override public String getMessage() {
        return "Expected a "+String.join("/", expectedTypes)+" query, got "+query;
    }
}
