package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

public class FSException extends RuntimeException {
    private @Nullable List<String> ids = null;

    public static FSException wrap(SparqlEndpoint endpoint, Throwable t) {
        return switch (t) {
            case FSException fs -> {
                fs.endpoint(endpoint);
                yield fs;
            }
            case IllegalStateException e -> {
                if (e.getClass() == IllegalStateException.class) {
                    var fs = new FSIllegalStateException(endpoint, e.getMessage());
                    fs.setStackTrace(e.getStackTrace());
                    yield fs;
                }
                yield  new FSIllegalStateException(endpoint, t.getMessage(), t);
            }
            default -> new FSException(endpoint, t.getMessage(), t);
        };
    }

    public FSException(String message) { this(message, null); }
    public FSException(String message, @Nullable Throwable cause) {super(message, cause);}
    public FSException(@Nullable SparqlEndpoint endpoint, String message) {
        this(endpoint, message, null);
    }
    public FSException(@Nullable SparqlEndpoint endpoint, String message,
                       @Nullable Throwable cause) {
        this(message, cause);
        this.endpoint(endpoint);
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
        if (ids == null)
            return message;
        var sb = new StringBuilder().append(message).append(". ");
        for (String id : ids) {
            if (!message.contains(id))
                sb.append(id).append(", ");
        }
        sb.setLength(sb.length()-2);
        return sb.length() == message.length() ? message : sb.toString();
    }

    @Override public String toString() {
        return getClass().getSimpleName()+ ": "+getMessage();
    }

    /**
     * Attach an identifying value to this exception. {@code name+'='+value} will be added to {@link #getMessage()}.
     * @param name a key for {@code value}. If there was a previous {@code id()} call with same
     *             {@code name} the previous value will be replaced with the one given in
     *             this call instead of adding a second entry for the same {@code name}
     * @param value a value to display in {@code name+'='+value}.
     */
    public void id(String name, @Nullable String value) {
        if (ids == null) ids = new ArrayList<>();
        int i = 0, n = ids.size(), nameLen = name.length();
        for (; i < n; ++i) {
            var old = ids.get(i);
            if (old.startsWith(name) && old.length() > nameLen && old.charAt(nameLen) == '=')
                break;
        }
        if (value == null) {
            if (i < n) ids.remove(i);
        } else {
            var display = name+'='+value;
            if (i < n) ids.set(i, display);
            else       ids.add(display);
        }
    }

    /**
     * Equivalent to {@link #id(String, String)} with {@code "endpoint"} and {@code endpoint}.
     *
     * @param endpoint The {@link SparqlEndpoint} to offer
     */
    public void endpoint(@Nullable SparqlEndpoint endpoint) {
        id("endpoint", endpoint == null ? null : endpoint.toString());
    }
}
