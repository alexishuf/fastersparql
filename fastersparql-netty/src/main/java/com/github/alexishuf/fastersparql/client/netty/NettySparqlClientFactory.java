package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.row.RowParser;

public class NettySparqlClientFactory implements SparqlClientFactory {
    @Override public String tag() {
        return "netty";
    }

    @Override public int order() {
        return -100;
    }

    @Override
    public <Row, Fragment> SparqlClient<Row, Fragment>
    createFor(SparqlEndpoint endpoint, RowParser<Row> rowParser,
              FragmentParser<Fragment> fragmentParser) {
        if (endpoint.protocol().isWebSocket())
            return new NettyWebSocketSparqlClient<>(endpoint, rowParser, fragmentParser);
        return new NettySparqlClient<>(endpoint, rowParser, fragmentParser);
    }
}
