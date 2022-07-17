package com.github.alexishuf.fastersparql.client.util;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FasterSparqlProperties {
    public static final String REACTIVE_QUEUE_CAPACITY   = "fastersparql.reactive.queue.capacity";
    public static final String CLIENT_MAX_QUERY_GET = "fastersparql.client.max-query-get";
    public static final String CLIENT_CONN_RETRIES = "fastersparql.client.conn.retries";
    public static final String CLIENT_CONN_TIMEOUT_MS = "fastersparql.client.conn.timeout-ms";
    public static final String CLIENT_SO_TIMEOUT_MS = "fastersparql.client.so.timeout-ms";
    public static final String CLIENT_CONN_RETRY_WAIT_MS = "fastersparql.client.conn.retry.wait-ms";
    public static final int DEF_REACTIVE_QUEUE_CAPACITY = 1024;
    public static final int DEF_CLIENT_MAX_QUERY_GET = 1024;
    public static final int DEF_CLIENT_CONN_RETRIES = 3;
    public static final int DEF_CLIENT_CONN_TIMEOUT_MS = 0;
    public static final int DEF_CLIENT_SO_TIMEOUT_MS = 0;
    public static final int DEF_CLIENT_CONN_RETRY_WAIT_MS = 1000;

    protected interface Parser<T> {
        T parse(String source, String value) throws IllegalArgumentException;
    }

    protected static <T> T readProperty(String propertyName, T defaultValue, Parser<T> parser) {
        String source = "JVM property "+propertyName;
        String value = System.getProperty(propertyName);
        if (value == null) {
            String envName = propertyName.toUpperCase().replace('.', '_');
            source = "Environment var "+envName;
            value = System.getenv(envName);
        }
        return value == null ? defaultValue : parser.parse(source, value);
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

    protected static String readTrimmedString(
            String propertyName,
            @SuppressWarnings("SameParameterValue") String defaultValue) {
        return readProperty(propertyName, defaultValue, (src, val) -> {
            if (val == null) throw new IllegalArgumentException(src+"=null: null not allowed");
            return val.trim();
        });
    }

    /**
     * The size of the queue used to convert {@link org.reactivestreams.Publisher}s into
     * {@link Iterable}s
     *
     * Default value is {@link FasterSparqlProperties#DEF_REACTIVE_QUEUE_CAPACITY}
     */
    public static @Positive int reactiveQueueCapacity() {
        return readPositiveInt(REACTIVE_QUEUE_CAPACITY, DEF_REACTIVE_QUEUE_CAPACITY);
    }

    /**
     * If no {@link SparqlMethod} is set, for queries sized below this value,
     * {@link SparqlMethod#GET} will be used since not all SPARQL endpoints support the other
     * methods. However, for queries above this size, {@link SparqlMethod#POST} will be used,
     * since large queries (especially after percent-encoding) may exceed fixed buffer sizes for
     * the first line in the HTTP request.
     *
     * The default value is {@link FasterSparqlProperties#DEF_CLIENT_MAX_QUERY_GET}.
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
     * ({@link FasterSparqlProperties#maxRetries()}).
     *
     * @param timeUnit the desired time unit of the time window.
     * @return a non-negative number of timeunits to wait before each each retry.
     */
    public static @NonNegative long retryWait(TimeUnit timeUnit) {
        int ms = readPositiveInt(CLIENT_CONN_RETRY_WAIT_MS, DEF_CLIENT_CONN_RETRY_WAIT_MS);
        return timeUnit.convert(ms, TimeUnit.MILLISECONDS);
    }
}
