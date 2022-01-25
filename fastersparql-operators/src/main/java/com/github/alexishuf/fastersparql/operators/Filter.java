package com.github.alexishuf.fastersparql.operators;

public interface Filter extends Operator {
    default OperatorName name() { return OperatorName.FILTER; }
}
