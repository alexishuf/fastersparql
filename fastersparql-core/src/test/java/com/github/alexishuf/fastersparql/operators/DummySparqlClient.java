package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DummySparqlClient<R,I,F> implements SparqlClient<R,I,F> {
    final RowType<R, I> rowType;
    final Class<F> fragmentClass;

    public DummySparqlClient(RowType<R, I> rowType, Class<F> fragmentClass) {
        this.rowType = rowType;
        this.fragmentClass = fragmentClass;
    }

    public DummySparqlClient(RowType<R, I> rowType) {
        //noinspection unchecked
        this(rowType, (Class<F>) Object.class);
    }

    @Override public RowType<R, I> rowType() { return rowType; }
    @Override public Class<F>  fragmentClass() { return fragmentClass; }
    @Override public SparqlEndpoint endpoint() { return SparqlEndpoint.parse("http://example.org/sparql"); }
    @Override public void close() { }

    @Override
    public BIt<R> query(SparqlQuery sparql, @Nullable BIt<R> bindings, @Nullable BindType bindType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Graph<F> queryGraph(SparqlQuery sparql) {
        throw new UnsupportedOperationException();
    }


}
