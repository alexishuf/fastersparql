package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.OperatorName;
import com.github.alexishuf.fastersparql.operators.Slice;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;

public interface SliceProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.SLICE; }
    @Override Slice create(long flags, RowOperations rowOperations);
}
