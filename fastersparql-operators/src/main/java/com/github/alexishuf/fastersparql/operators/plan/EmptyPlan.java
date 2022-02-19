package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EmptyPlan<R> implements Plan<R> {
    private final List<String> publicVars, allVars;
    private final Class<? super R> rowClass;

    public EmptyPlan(List<String> publicVars, List<String> allVars, Class<? super R> rowClass) {
        this.publicVars = publicVars;
        this.allVars = allVars;
        this.rowClass = rowClass;
    }

    public EmptyPlan(Class<? super R> rowClass) {
        this(Collections.emptyList(), Collections.emptyList(), rowClass);
    }

    public EmptyPlan() {
        this(Collections.emptyList(), Collections.emptyList(), Object.class);
    }

    @Override public List<String> publicVars() {
        return publicVars;
    }

    @Override public List<String> allVars() {
        return allVars;
    }

    @Override public Results<R> execute() {
        return new Results<>(publicVars, rowClass, new EmptyPublisher<>());
    }

    @Override public Plan<R> bind(Map<String, String> ignored) {
        return this;
    }
}
