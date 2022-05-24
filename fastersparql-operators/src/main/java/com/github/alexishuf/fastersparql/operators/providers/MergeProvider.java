package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.Merge;
import com.github.alexishuf.fastersparql.operators.OperatorName;

public interface MergeProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.MERGE; }
    @Override Merge create(long flags, RowOperations rowOperations);
}
