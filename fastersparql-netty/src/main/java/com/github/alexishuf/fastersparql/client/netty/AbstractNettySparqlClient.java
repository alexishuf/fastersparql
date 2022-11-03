package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;

public abstract class AbstractNettySparqlClient<R, I, F> implements SparqlClient<R, I, F> {
    protected final SparqlEndpoint endpoint;
    protected final RowType<R, I> rowType;
    protected final FragmentParser<F> fragParser;

    public AbstractNettySparqlClient(SparqlEndpoint ep, RowType<R, I> rowType,
                                     FragmentParser<F> fragParser) {
        this.endpoint = ep;
        this.rowType = rowType;
        this.fragParser = fragParser;
    }

    @Override public RowType<R, I>        rowType() { return rowType; }
    @Override public Class<F>       fragmentClass() { return fragParser.fragmentClass(); }
    @Override public SparqlEndpoint      endpoint() { return endpoint; }

    protected abstract String endpointString();

    @Override public String toString() {
        return getClass().getSimpleName().replace("SparqlClient", "")+'['+endpointString()+']';
    }
}
