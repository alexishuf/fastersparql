package com.github.alexishuf.fastersparql.client.model;

import java.util.Arrays;
import java.util.List;

public enum SparqlMethod {
    GET,
    POST,
    FORM,
    WS;

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
     * <p>
     * If this method is {@link SparqlMethod#GET}, this will be an empty string. For other methods, a valid, all-lowercase content-type without parameters will be returned.
     *
     * @return A non-null (but empty if {@code this == GET}) lower-case content type.
     */
    public String contentType() {
        return switch (this) {
            case GET -> "";
            case POST, WS -> "application/sparql-query";
            case FORM -> "application/x-www-form-urlencoded";
        };
    }

    /**
     * {@link List} wrapping {@link SparqlMethod#values()}.
     */
    public static final List<SparqlMethod> VALUES =
            List.of(values());
}
