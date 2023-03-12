package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.netty.NettySparqlClient;
import com.github.alexishuf.fastersparql.client.netty.NettyWsSparqlClient;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FSTest {
    private static final SparqlEndpoint ENDPOINT = SparqlEndpoint.parse("http://example.org/sparql");
    private static final SparqlEndpoint WS_ENDPOINT = SparqlEndpoint.parse("ws://example.org/sparql");

    @Test
    void testDefaultFactory() {
        try (var client = FS.clientFor(ENDPOINT)) {
            assertNotNull(client);
            assertTrue(client instanceof NettySparqlClient);
        }
    }

    @Test
    void testBadTag() {
        try (var client = FS.clientFor(ENDPOINT, "404")) {
            assertNotNull(client);
            assertTrue(client instanceof NettySparqlClient);
        }
    }

    @Test
    void testWs() {
        try (var client = FS.clientFor(WS_ENDPOINT)) {
            assertNotNull(client);
            assertTrue(client instanceof NettyWsSparqlClient);
        }
    }
}