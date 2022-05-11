package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.OperatorName;
import com.github.alexishuf.fastersparql.operators.Union;

public interface UnionProvider extends OperatorProvider{
    default OperatorName operatorName() { return OperatorName.UNION; }
    @Override Union create(long flags, RowOperations rowOperations);
}
