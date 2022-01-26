package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.OperatorName;
import com.github.alexishuf.fastersparql.operators.Project;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;

public interface ProjectProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.PROJECT; }

    @Override Project create(long flags, RowOperations rowOperations);
}
