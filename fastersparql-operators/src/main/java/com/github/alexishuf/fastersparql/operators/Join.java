package com.github.alexishuf.fastersparql.operators;

public interface Join extends Operator {
    default OperatorName name() { return OperatorName.JOIN; }
}
