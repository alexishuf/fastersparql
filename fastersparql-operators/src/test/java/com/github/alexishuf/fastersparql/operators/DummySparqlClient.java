package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DummySparqlClient<R,F> implements SparqlClient<R,F> {
    Class<R> rowClass;
    Class<F> fragmentClass;

    public DummySparqlClient(Class<R> rowClass, Class<F> fragmentClass) {
        this.rowClass = rowClass;
        this.fragmentClass = fragmentClass;
    }

    public DummySparqlClient(Class<R> rowClass) {
        //noinspection unchecked
        this(rowClass, (Class<F>) Object.class);
    }

    @Override public Class<R>       rowClass() { return rowClass; }
    @Override public Class<F>  fragmentClass() { return fragmentClass; }
    @Override public SparqlEndpoint endpoint() { return SparqlEndpoint.parse("http://example.org/sparql"); }
    @Override public void close() { }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration,
                            @Nullable Results<R> bindings, @Nullable BindType bindType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        throw new UnsupportedOperationException();
    }


}
