package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.util.ClientBindingBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;

public abstract class AbstractSparqlClient implements SparqlClient {
    protected final SparqlEndpoint endpoint;
    protected boolean bindingAwareProtocol;

    public AbstractSparqlClient(SparqlEndpoint ep) {
        this.endpoint = ep;
    }

    @Override public SparqlEndpoint endpoint() { return endpoint; }

    @Override public <B extends Batch<B>> BIt<B> query(BindQuery<B> q) {
        if (q.query.isGraph())
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        try {
            return new ClientBindingBIt<>(q, this);
        } catch (Throwable t) {
            throw FSException.wrap(endpoint, t);
        }
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
