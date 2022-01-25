package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration.SparqlConfigurationBuilder;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.UriUtils;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.net.InetAddress.getByName;

@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
@Slf4j
public final class SparqlEndpoint {
    /**
     * The full SPARQL endpoint URI, in conformance to
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396">RFC 2396</a>.
     *
     * SPARQL endpoint URIs are not allowed to contain fragment identifiers
     * (.e.g., {@code #author}).
     */
    @Getter @ToString.Include @EqualsAndHashCode.Include
    private final String uri;

    /**
     * A {@link URI} object for {@link SparqlEndpoint#uri()}.
     */
    @Getter private final URI toURI;

    /**
     * Whether the URI scheme is HTTP. If false, the URI is HTTP.
     */
    @Getter private final Protocol protocol;

    /**
     * The <a href="https://datatracker.ietf.org/doc/html/rfc2396#section-3.3">path</a> segment
     * of the URI optionally followed by a {@code ?} and the
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396#section-3.4">query</a> segment if
     * the URI has a query component (see {@link SparqlEndpoint#hasQuery()}).
     *
     * Neither the path nor the query segment have their percent-escapes decoded.
     *
     * The path segment will always start with a single {@code /}.
     */
    @Getter private final String rawPathWithQuery;

    /**
     * Whether the URI includes a non-empty
     * <a href="https://datatracker.ietf.org/doc/html/rfc2396#section-3.4">query</a> component.
     */
    @Getter private final boolean hasQuery;

    /**
     * The allowed methods and result media types, as well as headers and query parameters to
     * be set in queries.
     *
     * When executing a query against this endpoint, query-specific {@link SparqlConfiguration},
     * may be used so long as the given configuration
     * {@link SparqlConfiguration#isAcceptedBy(SparqlConfiguration)} the {@link SparqlEndpoint}
     * configuration. The effectively applied {@link SparqlConfiguration} will be the result of
     * {@link SparqlConfiguration#overlayWith(SparqlConfiguration)} called on the endpoint
     * configuration given the query-specific configuration.
     */
    @Getter @ToString.Include @EqualsAndHashCode.Include
    private final SparqlConfiguration configuration;

    private @Nullable AsyncTask<InetSocketAddress> resolved;

    /* --- --- --- Constants --- --- --- */

    private static final Pattern PATH_SLASHES = Pattern.compile("^/+");


    private static final String RDF_FORMATS_RX;
    private static final String RESULTS_FORMATS_RX;
    private static final String METHODS_RX;
    private static final Map<String, MediaType> RDF_FORMATS;
    private static final Map<String, SparqlResultFormat> RESULTS_FORMATS;
    private static final Map<String, SparqlMethod> METHODS;
    private static String toRx(Set<String> set) {
        StringBuilder b = new StringBuilder();
        for (String s : set) b.append(s).append('|');
        b.setLength(b.length()-1);
        return b.toString();
    }
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
        RDF_FORMATS_RX = toRx((RDF_FORMATS = rdfFormats).keySet());

        Map<String, SparqlResultFormat> resultFormats = new HashMap<>();
        for (SparqlResultFormat fmt : SparqlResultFormat.values())
            resultFormats.put(fmt.lowercase(), fmt);
        RESULTS_FORMATS_RX = toRx((RESULTS_FORMATS = resultFormats).keySet());

        Map<String, SparqlMethod> methods = new HashMap<>();
        for (SparqlMethod value : SparqlMethod.values())
            methods.put(value.lowercase(), value);
        METHODS_RX = toRx((METHODS = methods).keySet());
    }
    private static final Pattern AUGMENTED = Pattern.compile(
            "(?i)^((?:(?:^|,)(?:"+RESULTS_FORMATS_RX+"|"+RDF_FORMATS_RX+"|"+METHODS_RX+"))+)@");
    private static final Pattern LOOKS_AUGMENTED = Pattern.compile(
            "^\\s*[0-9a-z,.;+-]+https?:(//)?");


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
     * The given {@code uri} can be an augmented URI (see {@link SparqlEndpoint#parse(String)}),
     * in which case the configuration of the autgmented URI will be overlaid above the given
     * {@code configuration} parameter.
     *
     * @param uri the URI of the SPARQL endpoint or an augmented URI.
     * @param configuration The {@link SparqlConfiguration}. If null will use
     *        {@link SparqlConfiguration#EMPTY} instead. If {@code uri} is augmented,
     *                      the augmented URI configurations will be overlaid if compatible
     *                      if this.
     * @throws SparqlClientInvalidArgument if any of the following happens:
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
    public SparqlEndpoint(String uri, @Nullable SparqlConfiguration configuration) {
        if (uri == null)
            throw new SparqlClientInvalidArgument("Null URI");
        if (uri.isEmpty())
            throw new SparqlClientInvalidArgument("Empty URI");
        if (configuration == null)
            configuration = SparqlConfiguration.EMPTY;
        URI parsed;
        try {
            parsed = new URI(uri);
        } catch (URISyntaxException e) {
            SparqlConfigurationBuilder builder = SparqlConfiguration.builder();
            String plainUri = removeConfiguration(uri, builder);
            try {
                parsed = new URI(uri);
            } catch (URISyntaxException e2) {
                throw new SparqlClientInvalidArgument(e2.getMessage());
            }
            if (LOOKS_AUGMENTED.matcher(plainUri).find()) {
                throw new SparqlClientInvalidArgument("The URI "+uri+" looks augmented, but has" +
                        " invalid configurations. See SparqlEndpoint.parse() documentation.");
            } else {
                configuration = configuration.overlayAbove(builder.build());
                log.debug("Overlaying {} on constructor with augmented URI {}", configuration, uri);
            }
        }
        if (!parsed.isAbsolute())
            throw new SparqlClientInvalidArgument(uri+" is not an absolute URI");
        String scheme = parsed.getScheme();
        if (!scheme.startsWith("http"))
            throw new SparqlClientInvalidArgument(uri+"is not HTTP nor HTTPS");
        this.toURI = parsed;
        this.uri = uri;
        this.protocol = Protocol.fromURI(uri);
        String query = parsed.getRawQuery();
        this.hasQuery = query != null && !query.isEmpty();
        query = this.hasQuery ? "?"+query : "";
        String path = parsed.getRawPath();
        if (path == null || path.isEmpty())
            path = "/";
        else if (path.startsWith("/"))
            path = PATH_SLASHES.matcher(path).replaceFirst("/");
        else
            path = "/" + path;
        this.rawPathWithQuery = path + query;
        this.configuration = configuration;
    }

    /**
     * Parses a possibly augmented SPARQL endpoint URI.
     *
     * Augmented URIs have the form {@code OPTS@URI} where {@code URI} is defined by RFC 2396
     * and {@code OPTS} is a comma-separated list of the following keywords, in any order:
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
     * @param augmentedUri An RFC2396 URI or an URI prefixed with comma-separated list of options
     *                     and a '@' symbol.
     * @return The {@link SparqlEndpoint} object representing the endpoint at given URI with
     * the {@link SparqlConfiguration} built from the augmented URI keywords.
     */
    public static SparqlEndpoint parse(String augmentedUri) {
        SparqlConfigurationBuilder cfgBuilder = SparqlConfiguration.builder();
        String plainUri = removeConfiguration(augmentedUri, cfgBuilder);
        return new SparqlEndpoint(plainUri, cfgBuilder.build());
    }

    /* --- --- --- Computed properties --- --- --- */


    /**
     * Get the host as a DNS host name, an IP v4 IP address or an IPV6 address (enclosed in []).
     *
     * @return A non-null non-empty string naming the host.
     */
    public String host() { return toURI.getHost(); }

    /**
     * Get the port specified in the URI or the protocol default port.
     *
     * @return The port where the serve is listening, {@code > 0} and {@code < 65536}.
     */
    public int port() {
        int port = toURI.getPort();
        return port < 0 ? protocol().port() : port;
    }

    /**
     * Get the IP (v4 or v6) address for {@link SparqlEndpoint#host()}.
     *
     * If the URI uses a name instead of an IP address the name will be resolved and the
     * {@link AsyncTask} will complete upon resolution. The resolution may fail with
     * {@link UnknownHostException}.
     *
     * @return A {@link java.util.concurrent.Future} and {@link java.util.concurrent.CompletionStage}
     *         with the asynchronously resolved {@link InetSocketAddress} from the host and port
     *         in {@link SparqlEndpoint#uri()}.
     */
    public AsyncTask<InetSocketAddress> resolvedHost() {
        if (resolved == null)
            resolved = Async.async(() -> new InetSocketAddress(getByName(host()), port()));
        return resolved;
    }

    /**
     * Get the {@code user} in the {@code user:password} {@code userinfo} component of the URI,
     * if present.
     *
     * Percent-escapes will not be decoded.
     *
     * @return If there is an userinfo component in the URI a non-null percent-escaped,
     *         possibly empty string with the user, else {@code null}.
     */
    public @Nullable String rawUser() {
        String raw = toURI.getRawUserInfo();
        return raw == null ? null : raw.split(":")[0];
    }

    /**
     * Same as {@link SparqlEndpoint#rawUser()} but with percent escapes decoded.
     *
     * @return non-null string with decoded escapes or {@code null} if there is no {@code userinfo}.
     */
    public @Nullable String user() { return UriUtils.unescape(rawUser()); }

    /**
     * Get the {@code password} in the {@code user:password} if the URI has a {@code userinfo}
     * component.
     *
     * Percent-escapes will not be decoded. If there is no {@code :} in the {@code userinfo},
     * an empty string will be returned.
     *
     * @return A non-null percent-escaped string with the password.
     */
    public @Nullable String rawPassword() {
        String info = toURI.getRawUserInfo();
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
    public @Nullable String password() { return UriUtils.unescape(rawPassword()); }

    /* --- --- --- implementation details --- --- --- */

    private static String removeConfiguration(String augmentedUri,
                                              SparqlConfigurationBuilder builder) {
        Matcher m = AUGMENTED.matcher(augmentedUri);
        if (!m.find())
            return augmentedUri;
        for (String flag : m.group(1).toLowerCase().split(",")) {
            SparqlMethod method = METHODS.getOrDefault(flag, null);
            SparqlResultFormat resultFormat = RESULTS_FORMATS.getOrDefault(flag, null);
            MediaType rdfFormat = RDF_FORMATS.getOrDefault(flag, null);
            if      (method       != null) builder.method(method);
            else if (resultFormat != null) builder.resultsAccept(resultFormat);
            else if (rdfFormat    != null) builder.rdfAccept(rdfFormat);
        }
        return augmentedUri.substring(m.end());
    }
}
