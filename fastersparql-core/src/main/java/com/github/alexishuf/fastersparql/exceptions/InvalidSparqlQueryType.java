package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;

public class InvalidSparqlQueryType extends InvalidSparqlException {
    public InvalidSparqlQueryType(CharSequence query) {
        super("Unexpected query type: "+query.toString().replace("\n", "\\n"));
    }
}
