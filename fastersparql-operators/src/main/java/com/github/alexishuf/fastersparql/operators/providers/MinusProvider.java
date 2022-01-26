package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.Minus;
import com.github.alexishuf.fastersparql.operators.OperatorName;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;

public interface MinusProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.MINUS; }
    @Override Minus create(long flags, RowOperations rowOperations);
}