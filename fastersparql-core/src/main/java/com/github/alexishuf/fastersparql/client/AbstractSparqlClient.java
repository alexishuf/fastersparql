package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;

public abstract class AbstractSparqlClient<R,I,F> implements SparqlClient<R,I,F> {
    protected final SparqlEndpoint endpoint;
    protected final RowType<R, I> rowType;
    protected final FragmentParser<F> fragParser;

    public AbstractSparqlClient(SparqlEndpoint ep, RowType<R, I> rowType,
                                FragmentParser<F> fragParser) {
        this.endpoint = ep;
        this.rowType = rowType;
        this.fragParser = fragParser;
    }

    @Override public RowType<R, I>        rowType() { return rowType; }
    @Override public Class<F>       fragmentClass() { return fragParser.fragmentClass(); }
    @Override public SparqlEndpoint      endpoint() { return endpoint; }

    @Override public String toString() {
        String name = getClass().getSimpleName();
        if (name.endsWith("SparqlClient"))
            name = name.substring(0, name.length()-12);
        return name+'['+endpoint+']';
    }
}
