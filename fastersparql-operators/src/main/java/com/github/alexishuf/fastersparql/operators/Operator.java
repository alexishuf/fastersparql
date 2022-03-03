package com.github.alexishuf.fastersparql.operators;

public interface Operator {
    OperatorName name();
    <R> Class<R> rowClass();
}
