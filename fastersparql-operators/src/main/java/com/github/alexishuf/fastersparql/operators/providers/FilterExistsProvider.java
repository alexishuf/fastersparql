package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.FilterExists;
import com.github.alexishuf.fastersparql.operators.OperatorName;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;

public interface FilterExistsProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.FILTER_EXISTS; }
    @Override FilterExists create(long flags, RowOperations rowOperations);
}
