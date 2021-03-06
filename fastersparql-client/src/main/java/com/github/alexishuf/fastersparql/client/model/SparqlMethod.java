package com.github.alexishuf.fastersparql.client.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum SparqlMethod {
    GET,
    POST,
    FORM,
    WS;

    public boolean hasRequestBody() {
        return this != GET;
    }

    private static final String[] LOWERCASE =
            Arrays.stream(values()).map(m -> m.name().toLowerCase()).toArray(String[]::new);

    /**
     * Equivalent to {@link SparqlMethod#name()}{@code .toLowerCase()}.
     * @return A non-null lowercase string.
     */
    public String lowercase() {
        return LOWERCASE[ordinal()];
    }

    /**
     * Get the Content-Type for HTTP request bodies sent using this method.
     *
     * If this method is {@link SparqlMethod#GET}, this will be an empty string. For other methods, a valid, all-lowercase content-type without parameters will be returned.
     *
     * @return A non-null (but empty if {@code this == GET}) lower-case content type.
     */
    public String contentType() {
        switch (this) {
            case  GET: return "";
            case POST:
            case   WS: return "application/sparql-query";
            case FORM: return "application/x-www-form-urlencoded";
            default: throw new UnsupportedOperationException(this+" not supported");
        }
    }

    /**
     * {@link List} wrapping {@link SparqlMethod#values()}.
     */
    public static final List<SparqlMethod> VALUES =
            Collections.unmodifiableList(Arrays.asList(values()));
}
