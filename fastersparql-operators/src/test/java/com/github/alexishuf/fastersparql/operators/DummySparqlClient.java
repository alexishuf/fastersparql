package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DummySparqlClient<R,F> implements SparqlClient<R,F> {
    @Override public SparqlEndpoint endpoint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        throw new UnsupportedOperationException();
    }

    @Override public void close() { }
}
