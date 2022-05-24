package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Join;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Value @Accessors(fluent = true)
public class JoinPlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    String name;
    Class<? super R> rowClass;
    JoinPlan<R> parent;
    Join op;
    List<? extends Plan<R>> operands;

    @Builder
    public JoinPlan(@lombok.NonNull Join op, @lombok.NonNull Class<? super R> rowClass,
                    @Singular @lombok.NonNull List<? extends Plan<R>> operands,
                    @Nullable JoinPlan<R> parent, @Nullable String name) {
        this.name = name == null ? "Join-"+nextId.getAndIncrement() : name;
        this.rowClass = rowClass;
        this.parent = parent;
        this.op = op;
        this.operands = operands;
    }

    public JoinPlan<R> withOperands(List<? extends Plan<R>> operands) {
        if (operands == this.operands)
            return this;
        return new JoinPlan<>(op, rowClass, operands, parent, name);
    }

    @Override public List<String> publicVars() {
        return PlanHelpers.publicVarsUnion(operands);
    }

    @Override public List<String> allVars() {
        return PlanHelpers.allVarsUnion(operands);
    }

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public Plan<R> bind(Binding binding) {
        return new JoinPlan<>(op, rowClass, PlanHelpers.bindAll(operands, binding), this, name);
    }
}
