package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.LeftJoin;
import com.github.alexishuf.fastersparql.operators.OperatorName;

public interface LeftJoinProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.LEFT_JOIN; }
    @Override LeftJoin create(long flags, RowOperations rowOperations);
}
