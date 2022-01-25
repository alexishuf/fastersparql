package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.Minus;
import com.github.alexishuf.fastersparql.operators.OperatorName;

public interface MinusProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.MINUS; }
    @Override Minus create(long flags);
}
