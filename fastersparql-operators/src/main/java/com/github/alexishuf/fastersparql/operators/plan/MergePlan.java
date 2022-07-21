package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Merge;
import lombok.Builder;
import lombok.Singular;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class MergePlan<R> extends AbstractNAryPlan<R, MergePlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Merge op;


    @Builder
    public MergePlan(@lombok.NonNull  Class<? super R> rowClass,
                     @lombok.NonNull Merge op,
                     @Singular List<? extends Plan<R>> inputs,
                     @Nullable MergePlan<R> parent, @Nullable String name) {
        super(rowClass, inputs == null ? Collections.emptyList() : inputs,
              name == null ? "CollapsableUnion-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public           Merge        op()          { return op; }
    @Override public Results<R>   execute()     { return op.run(this); }

    @Override public MergePlan<R> bind(Binding binding) {
        return new MergePlan<>(rowClass, op, PlanHelpers.bindAll(operands, binding),
                              this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MergePlan)) return false;
        if (!super.equals(o)) return false;
        MergePlan<?> mergePlan = (MergePlan<?>) o;
        return op.equals(mergePlan.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
