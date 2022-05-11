package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DummySparqlClient<R,F> implements SparqlClient<R,F> {
    @Override public Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) Object.class;
    }

    @Override public Class<F> fragmentClass() {
        //noinspection unchecked
        return (Class<F>) Object.class;
    }

    @Override public SparqlEndpoint endpoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration,
                            @Nullable Results<R> bindings, @Nullable BindType bindType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        throw new UnsupportedOperationException();
    }

    @Override public void close() { }
}
