package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.LeftJoin;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Value @Accessors(fluent = true)
public class LeftJoinPlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    Class<? super R> rowClass;
    LeftJoin op;
    Plan<R> left, right;
    String name;

    @Builder
    public LeftJoinPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull LeftJoin op,
                        @lombok.NonNull Plan<R> left, @lombok.NonNull Plan<R> right,
                        @Nullable String name) {
        this.rowClass = rowClass;
        this.op = op;
        this.left = left;
        this.right = right;
        this.name = name == null ? "LeftJoin-"+nextId.getAndIncrement() : null;
    }

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public List<String> publicVars() {
        return VarUtils.union(left.publicVars(), right.publicVars());
    }

    @Override public List<String> allVars() {
        return VarUtils.union(left.allVars(), right.allVars());
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new LeftJoinPlan<>(rowClass, op, left.bind(var2ntValue), right.bind(var2ntValue), name);
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new LeftJoinPlan<>(rowClass, op, left.bind(vars, ntValues), right.bind(vars, ntValues), name);
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new LeftJoinPlan<>(rowClass, op, left.bind(vars, ntValues), right.bind(vars, ntValues), name);
    }
}
