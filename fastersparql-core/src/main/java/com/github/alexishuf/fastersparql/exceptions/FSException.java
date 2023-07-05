package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FSException extends RuntimeException {
    private @Nullable SparqlEndpoint endpoint;

    public static FSException wrap(SparqlEndpoint endpoint, Throwable t) {
        if (t == null) {
            return null;
        } else if (t instanceof FSException ce) {
            ce.offerEndpoint(endpoint);
            return ce;
        } else if (t.getClass() == IllegalStateException.class) {
            return new FSIllegalStateException(endpoint, t.getMessage());
        } else if (t instanceof IllegalStateException) {
            return new FSIllegalStateException(endpoint, t.getMessage(), t);
        } else {
            return new FSException(endpoint, t.getMessage(), t);
        }
    }

    public FSException(String message) { this(null, message, null); }
    public FSException(String message, @Nullable Throwable cause) {
        this(null, message, cause);
    }
    public FSException(@Nullable SparqlEndpoint endpoint, String message) {
        this(endpoint, message, null);
    }

    public FSException(@Nullable SparqlEndpoint endpoint, String message,
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
        String message = super.getMessage();
        if (message == null) {
            if (getCause() != null) {
                message = getCause().getMessage();
                if (message == null)
                    message = getCause().getClass().getSimpleName();
            } else {
                message = "<<no message nor causing exception>>";
            }
        }
        return endpoint == null ? message : includeEndpoint(message, endpoint);
    }

    @Override public String toString() {
        return getClass().getSimpleName()+ ": "+getMessage();
    }

    public @Nullable SparqlEndpoint endpoint() { return endpoint; }

    /**
     * Offers a new value for {@link FSException#endpoint()}.
     *
     * <p>The new value will be accepted only if there is no current endpoint or if the current
     * endpoint has an empty {@link SparqlConfiguration} and the offer has a non-empty
     * configuration on the same URI.</p>
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
