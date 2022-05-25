package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.net.URI;

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

    public static int port(URI uri) {
        int explicit = uri.getPort();
        return explicit <= 0 ? fromURI(uri).port() : explicit;
    }

    public static Protocol fromURI(URI uri) {
        String scheme = uri.getScheme();
        switch (scheme) {
            case  "http": return HTTP;
            case "https": return HTTPS;
            case    "ws": return WS;
            case   "wss": return WSS;
            default: throw new IllegalArgumentException("Unknown scheme "+ scheme);
        }
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
