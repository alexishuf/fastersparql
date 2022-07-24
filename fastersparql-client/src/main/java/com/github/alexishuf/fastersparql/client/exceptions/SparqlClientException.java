package com.github.alexishuf.fastersparql.client.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SparqlClientException extends RuntimeException {
    private @Nullable SparqlEndpoint endpoint;

    public SparqlClientException(String message) { this(null, message, null); }
    public SparqlClientException(String message, @Nullable Throwable cause) {
        this(null, message, cause);
    }
    public SparqlClientException(@Nullable SparqlEndpoint endpoint, String message) {
        this(endpoint, message, null);
    }

    public SparqlClientException(@Nullable SparqlEndpoint endpoint, String message,
                                 @Nullable Throwable cause) {
        super(message, cause);
        this.endpoint = endpoint;
    }

    private static String includeEndpoint(String message, SparqlEndpoint endpoint) {
        if (!message.contains(endpoint.uri()))
            return message + (message.endsWith(".") ? " " : ". ") + "Endpoint: "+endpoint;
        return message;
    }

    @Override public String getMessage() {
        String parent = super.getMessage();
        return endpoint == null ? parent : includeEndpoint(parent, endpoint);
    }

    public @Nullable SparqlEndpoint endpoint() { return endpoint; }

    /**
     * Offers a new value for {@link SparqlClientException#endpoint()}.
     *
     * The new value will be accepted only if there is no current endpoint or if the current
     * endpoint has an empty {@link SparqlConfiguration} and the offer has a non-empty
     * configuration on the same URI.
     *
     * @param offer The {@link SparqlEndpoint} to offer
     * @return whether the {@code offer} was set
     */
    @SuppressWarnings("UnusedReturnValue") public boolean offerEndpoint(SparqlEndpoint offer) {
        boolean accept = endpoint == null
                      || (!endpoint.equals(offer) && endpoint.configuration().isEmpty()
                                                  && !offer  .configuration().isEmpty());
        if (accept)
            endpoint = offer;
        return accept;
    }
}
