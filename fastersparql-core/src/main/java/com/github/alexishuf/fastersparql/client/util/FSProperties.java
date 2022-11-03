package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FSProperties {
    private static final Map<String, Object> PROP_CACHE = new ConcurrentHashMap<>();

    public static final String CLIENT_MAX_QUERY_GET = "fastersparql.client.max-query-get";
    public static final String CLIENT_CONN_RETRIES = "fastersparql.client.conn.retries";
    public static final String CLIENT_CONN_TIMEOUT_MS = "fastersparql.client.conn.timeout-ms";
    public static final String CLIENT_SO_TIMEOUT_MS = "fastersparql.client.so.timeout-ms";
    public static final String CLIENT_CONN_RETRY_WAIT_MS = "fastersparql.client.conn.retry.wait-ms";
    public static final String CLIENT_CONS_BATCH_MIN_WAIT_MS = "fastersparql.client.batch.min-wait-ms";

    public static final int DEF_CLIENT_MAX_QUERY_GET = 1024;
    public static final int DEF_CLIENT_CONN_RETRIES = 3;
    public static final int DEF_CLIENT_CONN_TIMEOUT_MS = 0;
    public static final int DEF_CLIENT_SO_TIMEOUT_MS = 0;
    public static final int DEF_CLIENT_CONN_RETRY_WAIT_MS = 1000;
    public static final int DEF_CLIENT_CONS_BATCH_MIN_WAIT_MS = 10;

    protected interface Parser<T> {
        T parse(String source, String value) throws IllegalArgumentException;
    }

    protected static <T> T readPropertyExternally(String propertyName, T defaultValue,
                                                  Parser<T> parser) {
        String source = "JVM property "+propertyName;
        String value = System.getProperty(propertyName);
        if (value == null) {
            String envName = propertyName.toUpperCase().replace('.', '_');
            source = "Environment var "+envName;
            value = System.getenv(envName);
        }
        return value == null ? defaultValue : parser.parse(source, value);
    }

    protected static <T> T readProperty(String propertyName, T defaultValue, Parser<T> parser) {
        //noinspection unchecked
        return (T) PROP_CACHE.computeIfAbsent(propertyName,
                                              k -> readPropertyExternally(k, defaultValue, parser));

    }

    private static final Pattern BOOL_RX =
            Pattern.compile("(?i)\\s*(?:(t|true|1|y|yes)|(f|false|0|n|no))\\s*");
    protected static boolean readBoolean(String propertyName, boolean defaultValue) {
        return readProperty(propertyName, defaultValue, (src, val) -> {
            Matcher m = BOOL_RX.matcher(val);
            if (!m.matches())
                throw new IllegalArgumentException(src+"="+val+" is not a boolean");
            return !m.group(1).isEmpty();
        });
    }

    protected static @Positive int readPositiveInt(String propertyName, int defaultValue) {
        return readProperty(propertyName, defaultValue, (src, val) -> {
            int i = -1;
            try { i = Integer.parseInt(val); } catch (NumberFormatException ignored) {}
            if (i < 1)
                throw new IllegalArgumentException(src+"="+val+" is not a positive integer");
            return i;
        });
    }

    /**
     * Drops all cached property values, causing properties to be re-read from
     * {@link System#getProperty(String)} and {@link System#getenv(String)}.
     */
    public static void refresh() {
        PROP_CACHE.clear();
    }

    /**
     * If no SparqlMethod is set, for queries sized below this value,
     * GET will be used since not all SPARQL endpoints support the other
     * methods. However, for queries above this size, POST will be used,
     * since large queries (especially after percent-encoding) may exceed fixed buffer sizes for
     * the first line in the HTTP request.
     *
     * <p>The default value is {@link FSProperties#DEF_CLIENT_MAX_QUERY_GET}.</p>
     */
    public static @Positive int maxQueryByGet() {
        return readPositiveInt(CLIENT_MAX_QUERY_GET, DEF_CLIENT_MAX_QUERY_GET);
    }

    /**
     * How many times a {@link SparqlClient} should retry opening a connection to an endpoint if
     * the connection was actively refused or timed out.
     *
     * @return A number {@code >= 0} indicating how many retries should be made. If zero,
     *         there will be only the initial connection attempt.
     */
    public static @NonNegative int maxRetries() {
        return readPositiveInt(CLIENT_CONN_RETRIES, DEF_CLIENT_CONN_RETRIES);
    }

    /**
     * Timeout, in millis, for establishing a TCP connection.
     *
     * @return Either {@code 0}, delegating the choice the underlying OS, or a value {@code > 0}
     *         with the number of milliseconds after which TCP connections that failed to complete
     *         the handshake are to be considered failed.
     */
    public static int connectTimeoutMs() {
        return readPositiveInt(CLIENT_CONN_TIMEOUT_MS, DEF_CLIENT_CONN_TIMEOUT_MS);
    }

    /**
     * Timeout in milliseconds for all socket operations other than connect.
     *
     * @return Either {@code 0}, delegating the choice to the underlying OS, or a value {@code >0}
     *         with the timeout in milliseconds to be set for non-connect socket operations.
     */
    public static int soTimeoutMs() {
        return readPositiveInt(CLIENT_SO_TIMEOUT_MS, DEF_CLIENT_SO_TIMEOUT_MS);
    }

    /**
     * How much time to wait before each of the connection retries
     * ({@link FSProperties#maxRetries()}).
     *
     * @param timeUnit the desired time unit of the time window.
     * @return a non-negative number of timeunits to wait before each retry.
     */
    public static @NonNegative long retryWait(TimeUnit timeUnit) {
        int ms = readPositiveInt(CLIENT_CONN_RETRY_WAIT_MS, DEF_CLIENT_CONN_RETRY_WAIT_MS);
        return timeUnit.convert(ms, TimeUnit.MILLISECONDS);
    }

    /**
     * The default value for {@link BIt#minWait(long, TimeUnit)} in iterators consumed by
     * {@link SparqlClient}. This value will only be used if the configured
     * {@link BIt#minWait(TimeUnit)} is smaller than the value of this property.
     *
     * <p>The main use scenario where a {@link SparqlClient} <strong>consumes</strong> an
     * iterator is in the implementation of bind join(-like) queries either over the standard
     * SPARQL protocol (HTTP) or over the custom WebSocket protocol</p>.
     *
     * <p>The default is the median latency for global fixed broadband internet as published by
     * <a href="https://www.speedtest.net/global-index">Ookla</a></p>.
     *
     * @param timeUnit The desired unit of the duration value.
     * @return a non-negative value to be passed to the {@link BIt#minWait(long, TimeUnit)}
     *         method of iterators consumed by {@link SparqlClient}s.
     */
    public static @NonNegative long consumedBatchMinWait(TimeUnit timeUnit) {
        int ms = readPositiveInt(CLIENT_CONS_BATCH_MIN_WAIT_MS, DEF_CLIENT_CONS_BATCH_MIN_WAIT_MS);
        return timeUnit.convert(ms, TimeUnit.MILLISECONDS);
    }
}
