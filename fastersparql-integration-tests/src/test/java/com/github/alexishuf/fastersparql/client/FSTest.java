package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.netty.NettySparqlClientFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FSTest {
    @Test
    void testDefaultFactory() {
        SparqlClientFactory factory = FS.factory();
        assertNotNull(factory);
        assertTrue(factory instanceof NettySparqlClientFactory);
    }

    @Test
    void testBadTag() {
        SparqlClientFactory factory = FS.factory("nonExisting");
        assertNotNull(factory);
        assertTrue(factory instanceof NettySparqlClientFactory);
    }
}