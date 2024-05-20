package com.github.alexishuf.fastersparql.store;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class StoreSparqlClientFactory implements SparqlClientFactory {
    private static final Logger log = LoggerFactory.getLogger(StoreSparqlClientFactory.class);
    @Override public String tag()      { return "store"; }
    @Override public int    order()    { return 100; }
    @Override public String toString() { return getClass().getSimpleName(); }

    @Override public boolean supports(SparqlEndpoint endpoint) {
        if (endpoint.protocol() != Protocol.FILE) return false;
        File f = endpoint.asFile();
        if (!f.isDirectory()) {
            log.debug("StoreSparqlClientFactory rejecting non-directory path {}", f);
            return false;
        }
        String[] contents = f.list();
        if (contents == null) {
            log.error("StoreSparqlClientFactory rejecting {}: could not list contents", f);
            return false;
        }
        boolean strings = false, spo = false;
        for (String name : contents) {
            switch (name) {
                case "strings" -> strings = true;
                case "spo" -> spo = true;
            }
        }
        if (!strings || !spo) {
            log.debug("StoreSparqlClientFactory rejecting {}: no strings/spo files", f);
            return false;
        }
        return true;
    }

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        return new StoreSparqlClient(endpoint);
    }
}
