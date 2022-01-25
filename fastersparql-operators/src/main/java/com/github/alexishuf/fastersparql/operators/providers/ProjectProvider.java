package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.OperatorName;
import com.github.alexishuf.fastersparql.operators.Project;

public interface ProjectProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.PROJECT; }

    @Override Project create(long flags);
}
