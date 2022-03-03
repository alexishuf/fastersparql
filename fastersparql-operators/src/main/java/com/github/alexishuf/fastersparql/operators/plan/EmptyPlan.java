package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
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
public class EmptyPlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    String name;
    List<String> publicVars, allVars;
    Class<? super R> rowClass;

    @Builder
    public EmptyPlan(@lombok.NonNull Class<? super R> rowClass,
                     @Nullable String name, @Nullable @Singular List<String> publicVars,
                     @Nullable @Singular List<String> allVars) {
        this.name = name == null ? "Empty-"+nextId.getAndIncrement() : name;
        this.publicVars = publicVars == null ? Collections.emptyList() : publicVars;
        this.allVars = allVars == null ? Collections.emptyList() : allVars;
        this.rowClass = rowClass;
    }

    @Override public Results<R> execute() {
        return new Results<>(publicVars, rowClass, new EmptyPublisher<>());
    }

    @Override public Plan<R> bind(Map<String, String> ignored) {
        return this;
    }
}
