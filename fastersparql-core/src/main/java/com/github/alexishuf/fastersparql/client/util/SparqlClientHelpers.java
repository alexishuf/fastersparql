package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.PooledMutableRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.github.alexishuf.fastersparql.FSProperties.maxQueryByGet;
import static com.github.alexishuf.fastersparql.client.model.SparqlMethod.GET;
import static com.github.alexishuf.fastersparql.util.UriUtils.escapeQueryParam;
import static com.github.alexishuf.fastersparql.util.UriUtils.needsEscape;
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
            return toString.apply(list.getFirst());
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
     * @param eff the eff {@link SparqlConfiguration} (will use the method and params)
     * @param sparql the SPARQL query to send (not url-encoded)
     * @return a non-null, non-empty string
     */
    public static String firstLine(SparqlEndpoint endpoint, SparqlConfiguration eff, Rope sparql) {
        char sep = endpoint.hasQuery() ? '&' : '?';
        Rope prefix = endpoint.rawPathWithQueryRope();
        return switch (eff.methods().getFirst()) {
            case GET     -> {
                try (var r = writeParams(prefix, sep, sparql, eff.params())) {
                    yield r.toString();
                }
            }
            case POST,WS -> {
                try (var b = writeParams(prefix, sep, null, eff.params())) {
                    yield b.toString();
                }
            }
            case FORM    -> endpoint.rawPathWithQuery();
            case FILE    -> throw new UnsupportedOperationException();
        };
    }

    /**
     * Build a form string for use as the body of a {@link SparqlMethod#FORM} request.
     *
     * @param sparql the sparql query, not yet url-encoded
     * @param params additional params to set
     * @return a non-null and non-empty string to be used as a request body for
     *         {@code application/x-www-form-urlencoded}
     */
    public static PooledMutableRope formString(Rope sparql, Map<?, ? extends Collection<?>> params) {
        return writeParams(FinalSegmentRope.EMPTY, '\0', sparql, params);
    }

    private static final FinalSegmentRope QUERY_PARAM = FinalSegmentRope.asFinal("query=");
    private static PooledMutableRope writeParams(Rope prefix, char firstSeparator, @Nullable Rope sparql,
                                                 Map<?, ? extends Collection<?>> params) {
        int capacity = prefix.len() + (sparql == null ? 0 : 7 + sparql.len()) + params.size()*32;
        var b = PooledMutableRope.getWithCapacity(capacity);
        b.append(prefix);
        char sep = firstSeparator;
        if (sparql != null) {
            escapeQueryParam((sep == 0 ? b : b.append(sep)).append(QUERY_PARAM), sparql);
            sep = '&';
        }
        for (var e : params.entrySet()) {
            for (var v : e.getValue()) {
                if (sep != 0) b.append(sep);
                if (sep == firstSeparator) sep = '&';
                assert !needsEscape(FinalSegmentRope.asFinal(e.getKey()));
                assert !needsEscape(FinalSegmentRope.asFinal(v));
                b.append(e.getKey()).append('=').append(v);
            }
        }
        return b;
    }

    /**
     * Creates a copy of {@code endpoint} only with result formats supported by {@code reg}.
     *
     * @param endpoint the {@link SparqlEndpoint}
     * @throws UnacceptableSparqlConfiguration {@link SparqlConfiguration#resultsAccepts()}
     *         would be empty on the returned endpoint (no requested results format is supported).
     */
    public static SparqlEndpoint withSupported(SparqlEndpoint endpoint,
                                               Collection<SparqlMethod> allowedMethods) {
        List<SparqlResultFormat> supportedFormats = new ArrayList<>();
        boolean changed = false;
        SparqlConfiguration request = endpoint.configuration();
        for (SparqlResultFormat fmt : request.resultsAccepts()) {
            if (ResultsParser.supports(fmt))
                supportedFormats.add(fmt);
            else
                changed = true;
        }
        if (supportedFormats.isEmpty()) {
            SparqlConfiguration offer = request.toBuilder().clearResultsAccepts()
                    .resultsAccepts(SparqlResultFormat.VALUES.stream()
                            .filter(ResultsParser::supports).collect(toList()))
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
