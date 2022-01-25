package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.Join;
import com.github.alexishuf.fastersparql.operators.OperatorName;

public interface JoinProvider extends OperatorProvider {
    default OperatorName operatorName() { return OperatorName.JOIN; }
    @Override Join create(long flags);
}
