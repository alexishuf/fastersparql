package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;

public class UnboundSparqlClient extends AbstractSparqlClient {
    private static final SparqlEndpoint UNBOUND_ENDPOINT
            = SparqlEndpoint.parse("http://unbound.example.org/sparql");
    public static final UnboundSparqlClient UNBOUND_CLIENT = new UnboundSparqlClient();

    private UnboundSparqlClient() {
        super(UNBOUND_ENDPOINT);
    }

    @Override public Guard retain() { return NoOpGuard.INSTANCE; }

    @Override protected void doClose() {}

    @Override protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> batchType, SparqlQuery sparql) {
        throw new UnsupportedOperationException("UnboundSparqlClient cannot answer any query!");
    }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
        throw new UnsupportedOperationException("UnboundSparqlClient cannot answer any query!");
    }

    @Override protected <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> bt, SparqlQuery sparql,
                                                               Vars rebindHint) {
        throw new UnsupportedOperationException("UnboundSparqlClient cannot answer any query!");
    }

    @Override protected <B extends Batch<B>> Emitter<B> doEmit(EmitBindQuery<B> query,
                                                               Vars rebindHint) {
        throw new UnsupportedOperationException("UnboundSparqlClient cannot answer any query!");
    }
}
