package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;

public class InvalidMediaType extends SparqlClientInvalidArgument {
    public InvalidMediaType(String message) {
        super(message);
    }
}
