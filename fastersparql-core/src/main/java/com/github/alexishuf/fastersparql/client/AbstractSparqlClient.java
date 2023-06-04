package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class AbstractSparqlClient implements SparqlClient {
    protected final SparqlEndpoint endpoint;
    protected boolean bindingAwareProtocol;

    public AbstractSparqlClient(SparqlEndpoint ep) {
        this.endpoint = ep;
    }

    @Override public SparqlEndpoint      endpoint() { return endpoint; }

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        return query(batchType, sparql, null, null);
    }

    @Override
    public <B extends Batch<B>>
    BIt<B> query(BatchType<B> batchType, SparqlQuery sparql, @Nullable BIt<B> bindings,
                            @Nullable BindType type, @Nullable JoinMetrics metrics) {
        if (bindings == null)
            return query(batchType, sparql);
        else if (type == null)
            throw new NullPointerException("bindings != null, but type is null!");
        if (sparql.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        try {
            return new ClientBindingBIt<>(bindings, type, this, sparql, metrics);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        }
    }

    @Override public final <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql,
                                                             @Nullable BIt<B> bindings, @Nullable BindType type) {
        return query(batchType, sparql, bindings, type, null);
    }

    @Override public final boolean usesBindingAwareProtocol() {
        return bindingAwareProtocol;
    }

    @Override public String toString() {
        String name = getClass().getSimpleName();
        if (name.endsWith("SparqlClient"))
            name = name.substring(0, name.length()-12);
        return name+'['+endpoint+']';
    }
}
