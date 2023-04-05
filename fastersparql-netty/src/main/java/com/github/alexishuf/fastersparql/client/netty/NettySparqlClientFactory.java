package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;

public class NettySparqlClientFactory implements SparqlClientFactory {
    @Override public String      tag() { return "netty"; }
    @Override public int       order() { return 100; }
    @Override public String toString() { return "NettySparqlClientFactory"; }

    @Override public boolean supports(SparqlEndpoint endpoint) {
        return endpoint.protocol() != Protocol.FILE;
    }

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        if (endpoint.protocol().isWebSocket())
            return new NettyWsSparqlClient(endpoint);
        return new NettySparqlClient(endpoint);
    }
}
