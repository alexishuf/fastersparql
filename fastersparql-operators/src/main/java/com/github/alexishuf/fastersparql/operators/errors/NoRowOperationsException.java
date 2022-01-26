package com.github.alexishuf.fastersparql.operators.errors;

public class NoRowOperationsException extends IllegalOperatorArgumentException {
    public NoRowOperationsException(Class<?> cls) {
        super("No RowOperations for "+cls+". Change the row class or register " +
              "a RowOperationsProvider via SPI");
    }
}
