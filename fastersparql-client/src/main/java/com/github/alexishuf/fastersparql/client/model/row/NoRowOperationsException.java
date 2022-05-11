package com.github.alexishuf.fastersparql.client.model.row;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;

public class NoRowOperationsException extends SparqlClientException {
    public NoRowOperationsException(Class<?> cls) {
        super("No RowOperations for "+cls+". Change the row class or register " +
              "a RowOperationsProvider via SPI");
    }
}
