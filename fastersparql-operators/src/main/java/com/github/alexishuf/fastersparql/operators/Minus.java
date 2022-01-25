package com.github.alexishuf.fastersparql.operators;

public interface Minus extends Operator {
    default OperatorName name() { return OperatorName.MINUS; }
}
