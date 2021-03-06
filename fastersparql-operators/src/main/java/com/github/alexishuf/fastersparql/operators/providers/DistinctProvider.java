package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.Distinct;
import com.github.alexishuf.fastersparql.operators.OperatorName;

public interface DistinctProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.DISTINCT; }
    @Override Distinct create(long flags, RowOperations rowOperations);
}
