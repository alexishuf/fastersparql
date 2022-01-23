package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import org.checkerframework.checker.nullness.qual.PolyNull;

/**
 * The protocol over which the SPARQL protocol will be used.
 */
public enum Protocol {
    HTTP,
    HTTPS;

    public int port() {
        switch (this) {
            case  HTTP: return 80;
            case HTTPS: return 443;
        }
        throw new UnsupportedOperationException("No port known for "+this);
    }

    public static @PolyNull Protocol fromURI(@PolyNull String uri) {
        if (uri == null)
            return null;
        if (uri.startsWith("https:"))
            return HTTPS;
        else if (uri.startsWith("http:"))
            return HTTP;
        throw new SparqlClientInvalidArgument("The URI "+uri+" is not HTTP not HTTPS");
    }
}
