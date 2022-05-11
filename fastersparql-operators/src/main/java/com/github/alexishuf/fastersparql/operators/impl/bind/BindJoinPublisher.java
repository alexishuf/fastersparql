package com.github.alexishuf.fastersparql.operators.impl.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.bind.BindPublisher;
import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.concurrent.atomic.AtomicInteger;

class BindJoinPublisher<R> extends BindPublisher<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);

    private static String name(BindType type) {
        String prefix;
        switch (type) {
            case       JOIN: prefix = "BindJoinPublisher-"; break;
            case  LEFT_JOIN: prefix = "LeftBindJoinPublisher-"; break;
            case     EXISTS: prefix = "BindExistsPublisher-"; break;
            case NOT_EXISTS: prefix = "BindNotExistsPublisher-"; break;
            case      MINUS: prefix = "MinusPublisher-"; break;
            default: throw new UnsupportedOperationException("Unexpected type="+type);
        }
        return prefix+nextId.getAndIncrement();
    }

    public BindJoinPublisher(RowOperations rowOps, Results<R> left, BindType joinType,
                             Plan<R> right, int bindConcurrency) {
        super(left.publisher(), bindConcurrency,
              new PlanMergerBinder<>(joinType, rowOps, left.vars(), right),
              name(joinType), null);
    }
}
