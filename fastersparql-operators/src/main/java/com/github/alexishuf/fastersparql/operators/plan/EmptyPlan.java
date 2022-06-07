package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.client.model.Results.empty;
import static java.util.Collections.emptyList;

@Getter @Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class EmptyPlan<R> extends AbstractPlan<R, EmptyPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final List<String> publicVars, allVars;

    @Builder
    public EmptyPlan(@lombok.NonNull Class<? super R> rowClass,
                     @Nullable String name, @Nullable @Singular List<String> publicVars,
                     @Nullable @Singular List<String> allVars) {
        super(rowClass, emptyList(), name == null ? "Empty-"+nextId.getAndIncrement() : name, null);
        this.publicVars = publicVars == null ? emptyList() : publicVars;
        this.allVars = allVars == null ? emptyList() : allVars;
    }

    @Override public    @Nullable Plan<R> parent()              { return null; }
    @Override public    Results<R>        execute()             { return empty(publicVars, rowClass); }
    @Override public    Plan<R>           bind(Binding binding) { return this; }
}
