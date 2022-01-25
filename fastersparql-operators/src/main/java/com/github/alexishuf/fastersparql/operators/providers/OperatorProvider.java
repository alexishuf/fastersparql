package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.Operator;
import com.github.alexishuf.fastersparql.operators.OperatorName;

public interface OperatorProvider {
    OperatorName operatorName();
    int bid(long flags);
    Operator create(long flags);
}
