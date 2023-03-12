package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.row.RowType;
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

    @Override public <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql) {
        return query(rowType, sparql, null, null);
    }

    @Override
    public <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql, @Nullable BIt<R> bindings,
                            @Nullable BindType type, @Nullable JoinMetrics metrics) {
        if (bindings == null || bindings instanceof EmptyBIt<R>)
            return query(rowType, sparql);
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

    @Override public final <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql,
                             @Nullable BIt<R> bindings, @Nullable BindType type) {
        return query(rowType, sparql, bindings, type, null);
    }

    @Override public final boolean usesBindingAwareProtocol() {
        return bindingAwareProtocol;
    }

    @Override public Graph queryGraph(SparqlQuery sparql) {
        String name = getClass().getSimpleName();
        throw new UnsupportedOperationException(name+" does not support queryGraph()");
    }

    @Override public String toString() {
        String name = getClass().getSimpleName();
        if (name.endsWith("SparqlClient"))
            name = name.substring(0, name.length()-12);
        return name+'['+endpoint+']';
    }
}
