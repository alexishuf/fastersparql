package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Merge;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Value @Accessors(fluent = true)
public class MergePlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    Class<? super R> rowClass;
    MergePlan<R> parent;
    Merge op;
    List<? extends Plan<R>> inputs;
    String name;

    @Builder
    public MergePlan(@lombok.NonNull  Class<? super R> rowClass,
                     @lombok.NonNull Merge op,
                     @Singular List<? extends Plan<R>> inputs,
                     @Nullable MergePlan<R> parent, @Nullable String name) {
        this.rowClass = rowClass;
        this.parent = parent;
        this.op = op;
        this.inputs = inputs == null ? Collections.emptyList() : inputs;
        this.name = name == null ? "CollapsableUnion-"+nextId.getAndIncrement() : name;
    }

    @Override public Results<R> execute() { return op.run(this); }
    @Override public List<String> publicVars() { return PlanHelpers.publicVarsUnion(inputs); }
    @Override public List<String> allVars() { return PlanHelpers.allVarsUnion(inputs); }

    @Override public MergePlan<R> bind(Binding binding) {
        return new MergePlan<>(rowClass, op, PlanHelpers.bindAll(inputs, binding),
                this, name);
    }
}
