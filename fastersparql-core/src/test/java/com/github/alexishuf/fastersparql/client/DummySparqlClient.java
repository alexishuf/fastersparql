package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

public class DummySparqlClient extends AbstractSparqlClient {
    public static final DummySparqlClient DUMMY = new DummySparqlClient(SparqlEndpoint.parse("http://localhost/sparql"));

    public DummySparqlClient(SparqlEndpoint endpoint) { super(endpoint); }

    @Override public void close() { }

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        throw new UnsupportedOperationException();
    }

    @Override public <B extends Batch<B>> BIt<B> query(BindQuery<B> q) {
        throw new UnsupportedOperationException();
    }
}
