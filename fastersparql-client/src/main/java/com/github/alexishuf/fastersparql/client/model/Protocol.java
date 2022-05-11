package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import org.checkerframework.checker.nullness.qual.PolyNull;

/**
 * The protocol over which the SPARQL protocol will be used.
 */
public enum Protocol {
    HTTP,
    HTTPS,
    WS,
    WSS;

    public Protocol underlying() {
        switch (this) {
            case  WS: return HTTP;
            case WSS: return HTTPS;
            default : return this;
             }
    }

    public boolean needsSsl() {
        return this == HTTPS || this == WSS;
    }

    public boolean isWebSocket() {
        return this == WS || this == WSS;
    }

    public int port() {
        switch (this) {
            case  HTTP:
            case    WS: return 80;
            case HTTPS:
            case   WSS: return 443;
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
        else if (uri.startsWith("wss:"))
            return WSS;
        else if (uri.startsWith("ws:"))
            return WS;
        throw new SparqlClientInvalidArgument("The URI "+uri+" does not use a supported scheme: http, https, ws or wss");
    }
}
