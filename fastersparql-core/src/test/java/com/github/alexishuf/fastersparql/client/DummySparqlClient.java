package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

public class DummySparqlClient extends AbstractSparqlClient {
    public static final DummySparqlClient DUMMY = new DummySparqlClient(SparqlEndpoint.parse("http://localhost/sparql"));

    public DummySparqlClient(SparqlEndpoint endpoint) { super(endpoint); }

    @Override public Guard retain() { return NoOpGuard.INSTANCE; }

    @Override protected void doClose() { }

    @Override public <B extends Batch<B>> BIt<B> doQuery(BatchType<B> batchType, SparqlQuery sparql) {
        throw new UnsupportedOperationException();
    }

    @Override public <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> q) {
        throw new UnsupportedOperationException();
    }

    @Override protected <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> bt, SparqlQuery sparql,
                                                               Vars rebindHint) {
        throw new UnsupportedOperationException();
    }

    @Override protected <B extends Batch<B>> Emitter<B> doEmit(EmitBindQuery<B> query,
                                                               Vars rebindHint) {
        throw new UnsupportedOperationException();
    }
}
