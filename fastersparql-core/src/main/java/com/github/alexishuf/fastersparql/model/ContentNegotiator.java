package com.github.alexishuf.fastersparql.model;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;

public class ContentNegotiator {
    private final MediaType[] supported;

    public ContentNegotiator(MediaType... supported) {
        this.supported = supported;
    }

    public MediaType[] supported() {
        return supported;
    }

    /**
     * The "most acceptable" {@link MediaType} compatible with {@code this.supported}, or {@code null}.
     *
     * <p>The {@code supported} {@link MediaType}s are considered in decreasing priority: If a
     * spec in {@code acceptHeaders} matches more than one {@code supported} {@link MediaType},
     * the leftmost {@link MediaType} will be preferred. Leftmost entries in {@code acceptHeaders}
     * are likely preferred over rightmost specs that have the same {@code q}-value. When a spec
     * does not include a {@code q}-value, it is assumed to be {@code 1} (the max). {@code q}-values
     * are only parsed up to 3 decimal digits, anything beyond that is treated as zeroes.</p>
     *
     * <p>When a spec sets a parameter {@code p} to {@code v}, a {@code supported}
     * {@link MediaType} will only be deemed a match if it does not set {@code p} or sets
     * {@code p} to {@code v} (both name and value comparisons are case-insensitive). When a spec
     * sets parameters unset by the matched supported {@link MediaType}, the {@link MediaType}
     * returned by this method will include the parameters set by the spec.</p>
     *
     * @param acceptHeaders An iterator to one or more HTTP {@code Accept} header values, each
     *                      value may include one or more media type specs separated by {@code ,}.
     * @return The "most acceptable" {@link MediaType} among {@code this.supported} with
     *         additional parameters set by the accepting media type spec, or {@code null} if
     *         none of the supported {@link MediaType}s are accepted.
     */
    public @Nullable MediaType select(Iterator<String> acceptHeaders) {
        MediaType matched = null;
        int matchedQ = 0;
        while (acceptHeaders.hasNext()) {
            String value = acceptHeaders.next();
            for (int i = 0, j, end = value.length(); i < end; i = j+1) {
                j = (j = value.indexOf(',', i)) == -1 ? end : j;
                for (MediaType offer : supported) {
                    var res = offer.acceptedBy(value, i, j);
                    if (res != null && res.q() > matchedQ) {
                        matched = res.withParams();
                        matchedQ = res.q();
                    }
                }
            }
        }
        return matched;
    }

    @Override public String toString() {
        return "ContentNegotiator"+Arrays.toString(supported);
    }

    @Override public boolean equals(Object obj) {
        return obj instanceof ContentNegotiator r&& Arrays.equals(supported, r.supported);
    }

    @Override public int hashCode() {
        return Arrays.hashCode(supported);
    }
}
