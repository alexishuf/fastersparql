package com.github.alexishuf.fastersparql.client.util.bind;

import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.MonoPublisher;

import java.util.List;

public final class NoFilterBinder<R> implements Binder<R> {
    private final List<String> vars;

    public NoFilterBinder(List<String> vars) { this.vars = vars; }

    @Override public FSPublisher<R>          bind(R row) { return new MonoPublisher<>(row); }
    @Override public List<String>           resultVars() { return vars; }
    @Override public Binder<R>      copyIfNotShareable() { return this; }
    @Override public String                   toString() { return "NoFilterBinder" + vars; }
}
