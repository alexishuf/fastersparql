package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum SparqlResultFormat {
    JSON,
    TSV,
    XML,
    CSV;

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
            Collections.unmodifiableList(Arrays.asList(values()));

    /* --- --- --- internals --- --- --- */

    private static final String[] mediaTypeStrings = {
            "application/sparql-results+json",
            "text/tab-separated-values",
            "application/sparql-results+xml",
            "text/csv"
    };
    private static final MediaType[] mediaTypes = Arrays.stream(mediaTypeStrings)
            .map(MediaType::parse).toArray(MediaType[]::new);
    private static final String[] LOWERCASE = Arrays.stream(values())
            .map(SparqlResultFormat::name).map(String::toLowerCase)
            .toArray(String[]::new);

}
