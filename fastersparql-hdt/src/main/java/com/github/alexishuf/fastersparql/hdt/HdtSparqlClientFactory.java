package com.github.alexishuf.fastersparql.hdt;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HdtSparqlClientFactory implements SparqlClientFactory {
    private static final Logger log = LoggerFactory.getLogger(HdtSparqlClientFactory.class);

    @Override public String      tag() { return "hdt"; }
    @Override public int       order() { return 100; }
    @Override public String toString() { return "HdtSparqlClientFactory"; }

    @Override public boolean supports(SparqlEndpoint endpoint) {
        if (endpoint.protocol() != Protocol.FILE) return false;
        File file = endpoint.asFile();
        String name = file.getName();
        if (name.length() > 4 && name.regionMatches(true, name.length()-4, ".hdt", 0, 4))
            return true;
        log.trace("HdtSparqlClientFactory rejecting non '.hdt'-terminated file {}", file);
        return false;
    }

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        return new HdtSparqlClient(endpoint);
    }
}
