package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.util.UriUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public final class SparqlEndpoint {

    private final String uri;
    private final SparqlConfiguration configuration;
    private final boolean hasQuery;
    private final Protocol protocol;
    private final String rawPathWithQuery;
    private final Rope rawPathWithQueryRope;
    private final int port;
    private @MonotonicNonNull URI toURI;

    /* --- --- --- trivial accessors --- --- --- */

    /**
     * The full SPARQL endpoint URI, in conformance to
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>.
     *
     * <p>SPARQL endpoint URIs are not allowed to contain fragment identifiers
     * (.e.g., {@code #author}).</p>
     */
    public String uri() { return uri; }

    /** {@link Protocol#fromURI(String)} of {@link SparqlEndpoint#uri()} */
    public Protocol protocol() { return protocol; }

    /**
     * The <a href="https://datatracker.ietf.org/doc/html/rfc2396#section-3.3">path</a> segment
     * of the URI optionally followed by a {@code ?} and the
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396#section-3.4">query</a> segment if
     * the URI has a query component (see {@link SparqlEndpoint#hasQuery()}).
     *
     * <p>Neither the path nor the query segment have their percent-escapes decoded.</p>
     *
     * <p>The path segment will always start with a single {@code /}.</p>
     */
    public String rawPathWithQuery() { return rawPathWithQuery; }

    /** {@link SparqlEndpoint#rawPathWithQuery()} as a {@link Rope} */
    public Rope rawPathWithQueryRope() { return rawPathWithQueryRope; }

    /** Get the {@link File} corresponding to this endpoint's file:// URI. */
    public File asFile() {
        String uri = UriUtils.unescape(this.uri), path;
        if      (uri.startsWith("file://")) path = uri.substring(7);
        else if (uri.startsWith("file:")  ) path = uri.substring(5);
        else throw new IllegalArgumentException("Not a file: URI");
        return new File(path);
    }

    /**
     * Whether the URI includes a non-empty
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396#section-3.4">query</a> component.
     */
    public boolean hasQuery() { return hasQuery; }
    /**
     * The allowed methods and result media types, as well as headers and query parameters to
     * be set in queries.
     *
     * <p>When executing a query against this endpoint, query-specific {@link SparqlConfiguration},
     * may be used so long as the given configuration
     * {@link SparqlConfiguration#isAcceptedBy(SparqlConfiguration)} the {@link SparqlEndpoint}
     * configuration. The effectively applied {@link SparqlConfiguration} will be the result of
     * {@link SparqlConfiguration#overlayWith(SparqlConfiguration)} called on the endpoint
     * configuration given the query-specific configuration.</p>
     */
    public SparqlConfiguration configuration() { return configuration; }


    /* --- --- --- Constants --- --- --- */

    private static final Map<String, MediaType> RDF_FORMATS;
    private static final Map<String, SparqlResultFormat> RESULTS_FORMATS;
    private static final Map<String, SparqlMethod> METHODS;
    static {
        Map<String, MediaType> rdfFormats = new HashMap<>();
        rdfFormats.put("ttl", new MediaType("text", "turtle"));
        rdfFormats.put("nt", new MediaType("application", "ntriples"));
        rdfFormats.put("ntriples", new MediaType("application", "ntriples"));
        rdfFormats.put("n3", new MediaType("text", "n3"));
        rdfFormats.put("trig", new MediaType("application", "trig"));
        rdfFormats.put("rdf", new MediaType("application", "rdf+xml"));
        rdfFormats.put("rdfxml", new MediaType("application", "rdf+xml"));
        rdfFormats.put("jsonld", new MediaType("application", "ld+json"));
        RDF_FORMATS = rdfFormats;

        Map<String, SparqlResultFormat> resultFormats = new HashMap<>();
        for (SparqlResultFormat fmt : SparqlResultFormat.values())
            resultFormats.put(fmt.lowercase(), fmt);
        RESULTS_FORMATS = resultFormats;

        Map<String, SparqlMethod> methods = new HashMap<>();
        for (SparqlMethod value : SparqlMethod.values())
            methods.put(value.lowercase(), value);
        METHODS = methods;
    }


    /* --- --- --- Constructor --- --- --- */

    /**
     * Equivalent to {@link SparqlEndpoint#SparqlEndpoint(String, SparqlConfiguration)}
     * with {@link SparqlConfiguration#EMPTY}.
     *
     * @param uri the URI of the SPARQL endpoint.
     */
    public SparqlEndpoint(String uri) {
        this(uri, SparqlConfiguration.EMPTY);
    }

    /**
     * Builds a {@link SparqlEndpoint} referring to the given URI with the given configuration.
     *
     * <p>The given {@code uri} can be an augmented URI (see {@link SparqlEndpoint#parse(String)}),
     * in which case the configuration of the augmented URI will be overlaid above the given
     * {@code configuration} parameter.</p>
     *
     * @param augUri the URI of the SPARQL endpoint or an augmented URI.
     * @param configuration The {@link SparqlConfiguration}. If null will use
     *                      {@link SparqlConfiguration#EMPTY} instead. If {@code uri} is
     *                      augmented, {@code configuration} will be overlaid above
     *                      the augmented URI configuration.
     * @throws FSInvalidArgument if any of the following happens:
     *  <ul>
     *      <li>Null or empty URI</li>
     *      <li>Invalid URI</li>
     *      <li>Invalid parameters in an augmented URI</li>
     *      <li>Relative URI</li>
     *      <li>Non-HTTP and non-HTTPS URI</li>
     *      <li>Given {@code configurations} parameter is not accepted by the configuration
     *          embedded in the augmented URI</li>
     *  </ul>
     */
    public SparqlEndpoint(String augUri, @Nullable SparqlConfiguration configuration) {
        if (augUri == null)
            throw new FSInvalidArgument("Null URI");
        if (augUri.isEmpty())
            throw new FSInvalidArgument("Empty URI");
        if (configuration == null)
            configuration = SparqlConfiguration.EMPTY;
        var builder = SparqlConfiguration.builder();
        String plainUri = removeConfiguration(augUri, builder);
        //noinspection StringEquality
        if (plainUri != augUri)  // augUri really was augmented
            configuration = configuration.overlayAbove(builder.build());

        this.uri = plainUri;
        this.protocol = Protocol.fromURI(plainUri);
        int path, schemaSepLen, schema = plainUri.indexOf(":"), len = plainUri.length();
        schemaSepLen = plainUri.regionMatches(schema, "://", 0, 3) ? 3 : 1;
        if (protocol == Protocol.FILE)
            path = schema+schemaSepLen;
        else if (schema == -1)
            throw new FSInvalidArgument("No scheme:// in "+augUri);
        else
            path = plainUri.indexOf('/', schema+schemaSepLen);
        if (path == -1) {
            this.rawPathWithQueryRope = new ByteRope(this.rawPathWithQuery = "/");
        } else {
            int e = path;
            while (e+1 < len && plainUri.charAt(e+1) == '/') ++e;
            this.rawPathWithQueryRope = new ByteRope(this.rawPathWithQuery = plainUri.substring(e));
        }
        this.hasQuery = this.rawPathWithQuery.indexOf('?') >= 0;
        if (protocol == Protocol.FILE) {
            this.port = 0;
        } else {
            int colon = path - 1;
            for (char c; colon > 0 && (c = plainUri.charAt(colon)) >= '0' && c <= '9'; ) --colon;
            if (colon > 0 && plainUri.charAt(colon) == ':') {
                this.port = Integer.parseInt(plainUri.substring(colon + 1, path));
                if (this.port >= 65_536)
                    throw new FSInvalidArgument("Port number " + this.port + " is too large in " + uri);
            } else {
                this.port = this.protocol.port();
            }
        }
        this.configuration = configuration;
    }

    /**
     * Parses a possibly augmented SPARQL endpoint URI.
     *
     * <p>Augmented URIs have the form {@code OPTS@URI} where {@code URI} is defined by RFC 2396
     * and {@code OPTS} is a comma-separated list of the following keywords, in any order:</p>
     * <ul>
     *     <li>Allowed {@link SparqlMethod} keywords: {@code get}, {@code post} and {@code form}</li>
     *     <li>{@link SparqlResultFormat} keywords: {@code json}, {@code tsv}, {@code xml},
     *         {@code csv}</li>
     *     <li>Shorthands for RDF serialization formats: {@code ttl}, {@code n3}, {@code nt},
     *         {@code ntriples}, {@code jsonld} or {@code rdfxml}</li>
     * </ul>
     *
     * The list of keywords need not be segmented as in the above, but the order of keywords
     * indicates their precedence. That is, if {@code tsv} appears before {@code json},
     * TSV results formatting will have a higher precedence.
     *
     * @param augmentedUri An RFC2396 URI or a URI prefixed with comma-separated list of options
     *                     and a '@' symbol.
     * @return The {@link SparqlEndpoint} object representing the endpoint at given URI with
     * the {@link SparqlConfiguration} built from the augmented URI keywords.
     */
    public static SparqlEndpoint parse(String augmentedUri) {
        return new SparqlEndpoint(augmentedUri, SparqlConfiguration.EMPTY);
    }

    /* --- --- --- Computed properties --- --- --- */

    /**
     * A {@link URI} object for {@link SparqlEndpoint#uri()}.
     */
    public URI toURI() {
        try {
            return toURI == null ? toURI = new URI(uri) : toURI;
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unexpected URISyntaxException for "+uri, e);
        }
    }

    /**
     * Generate an augmented URI that can be fed into {@link SparqlEndpoint#parse(String)}
     * and generate an {@link SparqlEndpoint} with the same {@link SparqlMethod} and
     * {@link SparqlResultFormat} configurations.
     *
     * <p><strong>Other configurations in {@link SparqlEndpoint#configuration()} have no
     * representation in the augmented uri, thus the result of {@link SparqlEndpoint#parse(String)}
     * on the returned URI may differ from {@code this}.</strong>.</p>
     *
     * @return an augmented URI representing this {@link SparqlEndpoint}.
     */
    public String augmentedUri() {
        if (!configuration.hasMethods() && !configuration.hasResultsAccepts())
            return uri();
        StringBuilder sb = new StringBuilder();
        if (configuration.hasMethods()) {
            for (SparqlMethod m : configuration.methods())
                sb.append(m.lowercase()).append(',');
            sb.setLength(sb.length()-1);
        }
        if (configuration.hasResultsAccepts()) {
            if (sb.length() > 0) sb.append(',');
            for (SparqlResultFormat fmt : configuration.resultsAccepts())
                sb.append(fmt.lowercase()).append(',');
            sb.setLength(sb.length()-1);
        }
        assert sb.length() > 0 : "Unexpected empty StringBuilder";
        return sb.append('@').append(uri()).toString();
    }

    /**
     * Get the host as a DNS host name, an IP v4 IP address or an IPV6 address (enclosed in []).
     *
     * @return A non-null non-empty string naming the host.
     */
    public String host() { return toURI().getHost(); }

    /**
     * Get the port specified in the URI or the protocol default port.
     *
     * @return The port where the serve is listening, {@code > 0} and {@code < 65536}.
     */
    public int port() { return port; }

    /**
     * Get the {@code user} in the {@code user:password} {@code userinfo} component of the URI,
     * if present.
     *
     * <p>Percent-escapes will not be decoded.</p>
     *
     * @return If there is an userinfo component in the URI a non-null percent-escaped,
     *         possibly empty string with the user, else {@code null}.
     */
    public @Nullable String rawUser() {
        String raw = toURI().getRawUserInfo();
        return raw == null ? null : raw.split(":")[0];
    }

    /**
     * Same as {@link SparqlEndpoint#rawUser()} but with percent escapes decoded.
     *
     * @return non-null string with decoded escapes or {@code null} if there is no {@code userinfo}.
     */
    public @Nullable String user() {
        String raw = rawUser();
        return raw == null ? null : UriUtils.unescape(raw);
    }

    /**
     * Get the {@code password} in the {@code user:password} if the URI has a {@code userinfo}
     * component.
     *
     * <p>Percent-escapes will not be decoded. If there is no {@code :} in the {@code userinfo},
     * an empty string will be returned.</p>
     *
     * @return A non-null percent-escaped string with the password.
     */
    public @Nullable String rawPassword() {
        String info = toURI().getRawUserInfo();
        if (info != null) {
            String[] parts = info.split(":");
            return parts.length > 1 ? parts[1] : parts[0];
        }
        return null;
    }

    /**
     * Same as {@link SparqlEndpoint#rawPassword()}, but with escapes decoded.
     *
     * @return non-null decoded password if there is an {@code userinfo} in the URI, else null
     */
    public @Nullable String password() {
        String raw = rawPassword();
        return raw == null ? null : UriUtils.unescape(raw);
    }

    /* --- --- --- java.lang.Object methods --- --- --- */

    @Override public String toString() {
        boolean trivial = !configuration.hasRdfAccepts()
                       && configuration.params().isEmpty()
                       && configuration.headers().isEmpty()
                       && configuration.appendHeaders().isEmpty();
        return trivial ? augmentedUri() : "<"+uri+">@"+ configuration;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparqlEndpoint that)) return false;
        return uri.equals(that.uri) && configuration.equals(that.configuration);
    }

    @Override public int hashCode() {
        return Objects.hash(uri, configuration);
    }

    /* --- --- --- implementation details --- --- --- */

    private static final int[] AUGMENTED_CHAR = Rope.alphabet(", ", Rope.Range.LETTER);
    private static String removeConfiguration(String augmentedUri,
                                              SparqlConfiguration.Builder builder) {
        int len = augmentedUri.length(), at = 0;
        for (char c; at < len && (c=augmentedUri.charAt(at)) < 128 && Rope.contains(AUGMENTED_CHAR, (byte)c); )
            ++at;
        if (at == len || augmentedUri.charAt(at) != '@')
            return augmentedUri;
        for (int consumed = 0; consumed < at; ) {
            int tagEnd = augmentedUri.indexOf(',', consumed);
            if (tagEnd == -1 || tagEnd > at) tagEnd = at;
            String tag = augmentedUri.substring(consumed, tagEnd).toLowerCase();
            consumed = tagEnd+1;
            var meth = METHODS.get(tag);
            if (meth != null) {
                builder.method(meth);
            } else {
                var fmt = RESULTS_FORMATS.get(tag);
                if (fmt != null) {
                    builder.resultsAccept(fmt);
                } else {
                    var mt = RDF_FORMATS.get(tag);
                    if (mt != null) {
                        builder.rdfAccept(mt);
                    } else {
                        throw new FSInvalidArgument("Unknown "+tag+" in augmented  (tag0,tag1,...@scheme://) section of "+augmentedUri);
                    }
                }
            }
        }
        return augmentedUri.substring(at+1);
    }
}
