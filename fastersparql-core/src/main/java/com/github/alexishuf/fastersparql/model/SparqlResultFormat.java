package com.github.alexishuf.fastersparql.model;

import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;

import java.util.Arrays;
import java.util.List;

public enum SparqlResultFormat {
    JSON,
    TSV,
    XML,
    CSV,
    WS;

    public static SparqlResultFormat fromMediaType(MediaType mt) {
        var fmt = switch (mt.type()) {
            case "application" -> switch (mt.subtype()) {
                case "sparql-results+json" -> JSON;
                case "sparql-results+xml"  -> XML;
                case "websocket-sparql-results+tsv"  -> WS;
                default -> null;
            };
            case "text" -> switch (mt.subtype()) {
                case "tab-separated-values", "tsv" -> TSV;
                case "csv"                         -> CSV;
                default -> null;
            };
            default -> null;
        };
        if (fmt == null) throw new FSInvalidArgument("No known format for "+mt);
        return fmt;
    }

    /* --- --- --- methods --- --- --- */

    /**
     * Content-type without parameters for the format.
     *
     * @return Non-null, non-empty, lower-case content-type without parameters.
     */
    public String contentType() { return mediaTypeStrings[ordinal()]; }

    /**
     * {@link MediaType} corresponding to this {@link SparqlResultFormat}.
     *
     * @return Non-null {@link MediaType} instance.
     */
    @SuppressWarnings("unused")
    public MediaType asMediaType() { return mediaTypes[ordinal()]; }

    /**
     * lower-case version of {@link SparqlResultFormat#name()}.
     *
     * @return non-null and non-empty lower-case version of {@link SparqlResultFormat#name()}
     */
    public String lowercase() { return LOWERCASE[ordinal()]; }


    /* --- --- --- constants --- --- --- */

    /**
     * Get a {@link List} with all members of the enum
     */
    public static final List<SparqlResultFormat> VALUES =
            List.of(values());

    /* --- --- --- internals --- --- --- */

    private static final String[] mediaTypeStrings = {
            "application/sparql-results+json",
            "text/tab-separated-values",
            "application/sparql-results+xml",
            "text/csv",
            "application/x.websocket-sparql-results+tsv"
    };
    private static final MediaType[] mediaTypes = Arrays.stream(mediaTypeStrings)
            .map(MediaType::parse).toArray(MediaType[]::new);
    private static final String[] LOWERCASE = Arrays.stream(values())
            .map(SparqlResultFormat::name).map(String::toLowerCase)
            .toArray(String[]::new);

}
