package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class Tdb2SparqlClientFactory implements SparqlClientFactory {
    private static final Logger log = LoggerFactory.getLogger(Tdb2SparqlClientFactory.class);

    @Override public String toString() { return getClass().getSimpleName(); }
    @Override public String      tag() {return "tdb2";}
    @Override public int       order() {return 100;}

    @Override public boolean supports(SparqlEndpoint endpoint) {
        if (endpoint.protocol() != Protocol.FILE)
            return false;
        File file = endpoint.asFile();
        if (!file.isDirectory())
            return false;
        File[] generations = file.listFiles(f -> f.isDirectory() && f.getName().startsWith("Data-"));
        if (generations == null) {
            log.error("IO error reading contents of {}, treating as if not a TDB2 dir", file);
            return false;
        }
        if (generations.length == 0) {
            log.debug("No Data-* dirs in {}: not a TDB2", file);
            return false;
        }
        return true;
    }

    @Override public SparqlClient createFor(SparqlEndpoint endpoint) {
        return new Tdb2SparqlClient(endpoint);
    }
}
