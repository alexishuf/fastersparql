package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public class UnboundSparqlClient extends AbstractSparqlClient {
    private static final SparqlEndpoint UNBOUND_ENDPOINT
            = SparqlEndpoint.parse("http://unbound.example.org/sparql");
    public static final UnboundSparqlClient UNBOUND_CLIENT = new UnboundSparqlClient();

    private UnboundSparqlClient() {
        super(UNBOUND_ENDPOINT);
    }

    @Override
    public <B extends Batch<B>>
    BIt<B> query(BatchType<B> batchType, SparqlQuery sparql, @Nullable BIt<B> bindings,
                 @Nullable BindType type, @Nullable Metrics.JoinMetrics metrics) {
        throw new UnsupportedOperationException("UnboundSparqlClient cannot answer any query!");
    }

    @Override public void close() { }
}
