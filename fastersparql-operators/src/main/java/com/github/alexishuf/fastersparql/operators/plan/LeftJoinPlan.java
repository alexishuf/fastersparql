package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.LeftJoin;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

@Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class LeftJoinPlan<R> extends AbstractNAryPlan<R, LeftJoinPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    @Getter private final LeftJoin op;

    @Builder
    public LeftJoinPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull LeftJoin op,
                        @lombok.NonNull Plan<R> left, @lombok.NonNull Plan<R> right,
                        @Nullable LeftJoinPlan<R> parent, @Nullable String name) {
        super(rowClass, Arrays.asList(left, right),
              name == null ? "LeftJoin-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public Plan<R>                    left() { return operands.get(0); }
    public Plan<R>                   right() { return operands.get(1); }
    @Override public Results<R>    execute() { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new LeftJoinPlan<>(rowClass, op, operands.get(0).bind(binding),
                                  operands.get(1).bind(binding), this, name);
    }
}
