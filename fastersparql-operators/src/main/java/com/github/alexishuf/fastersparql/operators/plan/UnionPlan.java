package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Union;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Value  @Accessors(fluent = true)
public class UnionPlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    Class<? super R> rowClass;
    Union op;
    List<? extends Plan<R>> inputs;
    String name;

    @Builder
    public UnionPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Union op,
                     @Singular List<? extends Plan<R>> inputs, @Nullable String name) {
        this.rowClass = rowClass;
        this.op = op;
        this.inputs = inputs == null ? Collections.emptyList() : inputs;
        this.name = name == null ? "Union-"+nextId : name;
    }

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public List<String> publicVars() {
        return PlanHelpers.publicVarsUnion(inputs);
    }

    @Override public List<String> allVars() {
        return PlanHelpers.allVarsUnion(inputs);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new UnionPlan<>(rowClass, op, PlanHelpers.bindAll(inputs, var2ntValue), name);
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new UnionPlan<>(rowClass, op, PlanHelpers.bindAll(inputs, vars, ntValues), name);
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new UnionPlan<>(rowClass, op, PlanHelpers.bindAll(inputs, vars, ntValues), name);
    }
}
