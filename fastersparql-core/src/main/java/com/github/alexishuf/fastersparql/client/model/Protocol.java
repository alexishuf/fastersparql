package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import com.github.alexishuf.fastersparql.client.util.UriUtils;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.io.File;
import java.net.URI;

/**
 * The protocol over which the SPARQL protocol will be used.
 */
public enum Protocol {
    FILE,
    HTTP,
    HTTPS,
    WS,
    WSS;

    public boolean needsSsl() {
        return this == HTTPS || this == WSS;
    }

    public boolean isWebSocket() {
        return this == WS || this == WSS;
    }

    public int port() {
        return switch (this) {
            case HTTP, WS -> 80;
            case HTTPS, WSS -> 443;
            case FILE -> 0;
        };
    }

    public static int port(URI uri) {
        int explicit = uri.getPort();
        return explicit <= 0 ? fromURI(uri).port() : explicit;
    }

    public static Protocol fromURI(URI uri) {
        String scheme = uri.getScheme();
        return switch (scheme) {
            case "http" -> HTTP;
            case "https" -> HTTPS;
            case "ws" -> WS;
            case "wss" -> WSS;
            case "file" -> FILE;
            default -> throw new IllegalArgumentException("Unknown scheme " + scheme);
        };
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
        else if (uri.startsWith("file:"))
            return FILE;
        throw new SparqlClientInvalidArgument("The URI "+uri+" does not use a supported scheme: http, https, ws or wss");
    }

    public static File parseFile(String uri) {
        String path;
        String unescaped = UriUtils.unescape(uri);
        if      (unescaped.startsWith("file://")) path = unescaped.substring(7);
        else if (unescaped.startsWith("file:")  ) path = unescaped.substring(5);
        else throw new IllegalArgumentException("Not a file: URI");
        return new File(path);
    }
}
