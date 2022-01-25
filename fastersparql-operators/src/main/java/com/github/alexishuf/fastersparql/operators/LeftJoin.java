package com.github.alexishuf.fastersparql.operators;

public interface LeftJoin extends Operator {
    default OperatorName name() { return OperatorName.LEFT_JOIN; }
}
