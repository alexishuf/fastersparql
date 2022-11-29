package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;

public class NettySparqlClientFactory implements SparqlClientFactory {
    @Override public String tag() {
        return "netty";
    }

    @Override public int order() {
        return 100;
    }

    @Override public boolean supports(SparqlEndpoint endpoint) {
        return endpoint.protocol() != Protocol.FILE;
    }

    @Override
    public <R, I, F> SparqlClient<R, I, F> createFor(SparqlEndpoint endpoint, RowType<R, I> rowType, FragmentParser<F> fragmentParser) {
        if (endpoint.protocol().isWebSocket())
            return new NettyWebSocketSparqlClient<>(endpoint, rowType, fragmentParser);
        return new NettySparqlClient<>(endpoint, rowType, fragmentParser);
    }

    @Override public String toString() { return "NettySparqlClientFactory"; }
}
