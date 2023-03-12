package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public class DummySparqlClient extends AbstractSparqlClient {
    public static final DummySparqlClient DUMMY = new DummySparqlClient(SparqlEndpoint.parse("http://localhost/sparql"));

    public DummySparqlClient(SparqlEndpoint endpoint) { super(endpoint); }

    @Override public void close() { }

    @Override public <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql, @Nullable BIt<R> bindings,
                            @Nullable BindType type, @Nullable Metrics.JoinMetrics metrics) {
        throw new UnsupportedOperationException();
    }
}
