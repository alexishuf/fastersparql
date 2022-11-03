package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.client.parser.results.ResultsParserRegistry;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;

import java.util.*;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.client.model.SparqlMethod.*;
import static com.github.alexishuf.fastersparql.client.util.FSProperties.maxQueryByGet;
import static com.github.alexishuf.fastersparql.client.util.UriUtils.escapeQueryParam;
import static com.github.alexishuf.fastersparql.client.util.UriUtils.needsEscape;
import static java.util.stream.Collectors.toList;

/**
 * Helpers for implementers of {@link com.github.alexishuf.fastersparql.client.SparqlClient}
 */
public class SparqlClientHelpers {
    private static <T> String buildAcceptString(
            @MinLen(1) List<T> list, Function<T, ? extends String> toString,
            int singleMTSizeHint) {
        int size = list.size();
        assert size > 0;
        if (size == 1)
            return toString.apply(list.get(0));
        double step = size < 10 ? 0.1 : size < 20 ? 0.5 : 0.01;
        String format = step <= 0.01 ? "; q=%.2f, " : "; q=%.1f, ";

        // +9 <---> "; q=0.x, "
        StringBuilder b = new StringBuilder(size * (singleMTSizeHint + 7));
        double q = 1.0;
        for (int i = 0; i < size; i++, q -= step)
            b.append(toString.apply(list.get(i))).append(i > 0 ? String.format(format, q) : ", ");
        b.setLength(b.length() - 2);
        return b.toString();
    }

    /**
     * Builds a string for use as value of the {@code Accept} HTTP header.
     * <p>
     * q-values are introduced to the comma-separated list for all but the first (implicitly q=1.0)
     * such that the list order expresses priority.
     *
     * @param formats the list of {@link SparqlResultFormat}s, from most preferred to least preferred.
     * @return a non-null, non-empty accept string.
     */
    public static String resultsAcceptString(@MinLen(1) List<SparqlResultFormat> formats) {
        if (formats.isEmpty())
            throw new IllegalArgumentException("empty formats list");
        return buildAcceptString(formats, SparqlResultFormat::contentType, 31);
    }

    /**
     * Builds a string for use as value of the {@code Accept} HTTP header.
     * <p>
     * q-values are introduced to the comma-separated list for all but the first (implicitly q=1.0)
     * such that the list order expresses priority.
     *
     * @param types the list of types, from most preferred to least preferred.
     * @return a non-null, non-empty accept string.
     */
    public static @MinLen(1) String rdfAcceptString(@MinLen(1) List<MediaType> types) {
        if (types.isEmpty())
            throw new IllegalArgumentException("empty formats list");
        return buildAcceptString(types, Object::toString, 21);
    }

    /**
     * If {@code queryLen} exceeds {@link FSProperties#maxQueryByGet()}, choose
     * the second preferred method from {@code cfg}, else pick the first.
     */
    public static SparqlMethod method(SparqlConfiguration cfg, int queryLen) {
        SparqlMethod method = cfg.methods().get(0);
        if (method == GET && cfg.methods().size() > 1 && queryLen > maxQueryByGet())
            method = cfg.methods().get(1);
        return method;
    }

    /**
     * Get the string that follows the HTTP method name on the first line of an HTTP request.
     *
     * @param endpoint the target endpoint
     * @param effective the effective {@link SparqlConfiguration} (will use the method and params)
     * @param sparql the SPARQL query to send (not url-encoded)
     * @return a non-null, non-empty string
     */
    public static CharSequence firstLine(SparqlEndpoint endpoint, SparqlConfiguration effective,
                                         CharSequence sparql) {
        char firstSep = endpoint.hasQuery() ? '&' : '?';
        String prefix = endpoint.rawPathWithQuery();
        if (effective.methods().get(0) == GET) {
            return writeParams(prefix, firstSep, sparql, effective.params());
        } else if (effective.methods().get(0) == POST) {
            return writeParams(prefix, firstSep, null, effective.params());
        } else {
            assert effective.methods().get(0) == FORM;
            return prefix;
        }
    }

    /**
     * Build a form string for use as the body of a {@link SparqlMethod#FORM} request.
     *
     * @param sparql the sparql query, not yet url-encoded
     * @param otherParams additional params to set
     * @return a non-null and non-empty string to be used as a request body for
     *         {@code application/x-www-form-urlencoded}
     */
    public static CharSequence
    formString(CharSequence sparql,
               @Nullable Map<? extends CharSequence,
                             ? extends Collection<? extends CharSequence>> otherParams) {
        return writeParams("", '\0', sparql, otherParams);
    }

    private static CharSequence
    writeParams(CharSequence prefix, char firstSeparator, @Nullable CharSequence sparql,
                @Nullable Map<? extends CharSequence,
                              ? extends Collection<? extends CharSequence>> params) {
        params = params == null ? Collections.emptyMap() : params;
        int capacity = prefix.length() + (sparql == null ? 0 : 7 + sparql.length()*2);
        for (Map.Entry<? extends CharSequence, ? extends Collection<? extends CharSequence>> e
                : params.entrySet()) {
            capacity += e.getKey().length() + 1;
            for (CharSequence v : e.getValue())
                capacity += v.length()*2;
        }
        if (capacity == 0)
            return "";
        boolean first = true;
        StringBuilder output = new StringBuilder(capacity);
        output.append(prefix);
        if (sparql != null) {
            if (firstSeparator != '\0') output.append(firstSeparator);
            first = false;
            escapeQueryParam(output.append("query="), sparql);
        }
        for (Map.Entry<? extends CharSequence, ? extends Collection<? extends CharSequence>> e
                : params.entrySet()) {
            for (CharSequence v : e.getValue()) {
                if (first) {
                    if (firstSeparator != '\0') output.append(firstSeparator);
                    first = false;
                } else {
                    output.append('&');
                }
                assert !needsEscape(e.getKey());
                assert !needsEscape(v);
                output.append(e.getKey()).append('=').append(v);
            }
        }
        return output;
    }

    /**
     * Creates a copy of {@code endpoint} only with result formats supported by {@code reg}.
     *
     * @param endpoint the {@link SparqlEndpoint}
     * @param reg where to test which {@link SparqlConfiguration#resultsAccepts()} are supported.
     * @return a copy of {@code endpoint} where {@link ResultsParserRegistry#canParse(MediaType)}
     *         is {@code true} for all {@link SparqlConfiguration#resultsAccepts()}
     * @throws UnacceptableSparqlConfiguration {@link SparqlConfiguration#resultsAccepts()}
     *         would be empty on the returned endpoint (no requested results format is supported).
     */
    public static SparqlEndpoint withSupported(SparqlEndpoint endpoint, ResultsParserRegistry reg,
                                               Collection<SparqlMethod> allowedMethods) {
        List<SparqlResultFormat> supportedFormats = new ArrayList<>();
        boolean changed = false;
        SparqlConfiguration request = endpoint.configuration();
        for (SparqlResultFormat fmt : request.resultsAccepts()) {
            if (reg.canParse(fmt.asMediaType()))
                supportedFormats.add(fmt);
            else
                changed = true;
        }
        if (supportedFormats.isEmpty()) {
            SparqlConfiguration offer = request.toBuilder().clearResultsAccepts()
                    .resultsAccepts(SparqlResultFormat.VALUES.stream()
                            .filter(f -> reg.canParse(f.asMediaType())).collect(toList()))
                    .build();
            String msg = "None of the given SparqlResultFormats " + request.resultsAccepts() +
                         " has a ResultsParser in ResultsParserRegistry.get()";
            throw new UnacceptableSparqlConfiguration(endpoint.uri(), offer, request, msg);
        }
        List<SparqlMethod> supportedMethods = new ArrayList<>(endpoint.configuration().methods());
        changed |= supportedMethods.removeIf(m -> !allowedMethods.contains(m));
        if (changed) {
            SparqlConfiguration amended = request.toBuilder()
                    .clearMethods().methods(supportedMethods)
                    .clearResultsAccepts().resultsAccepts(supportedFormats)
                    .build();
            return new SparqlEndpoint(endpoint.uri(), amended);
        }
        return endpoint;
    }
}
