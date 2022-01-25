package com.github.alexishuf.fastersparql.operators;

public interface Union extends Operator {
    default OperatorName name() { return OperatorName.UNION; }
}
