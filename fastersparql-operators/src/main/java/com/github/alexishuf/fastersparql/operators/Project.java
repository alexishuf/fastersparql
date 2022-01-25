package com.github.alexishuf.fastersparql.operators;

public interface Project extends Operator {
    default OperatorName name() { return OperatorName.PROJECT; }
}
