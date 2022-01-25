package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.Filter;
import com.github.alexishuf.fastersparql.operators.OperatorName;

public interface FilterProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.FILTER; }
    @Override Filter create(long flags);
}
