package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.fed.selectors.AskSelector;
import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.NoneJoinReorderStrategy;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class FSProperties {
    private static final Map<String, Object> PROP_CACHE = new ConcurrentHashMap<>();

    /* --- --- --- property names --- --- --- */
    public static final String CLIENT_MAX_QUERY_GET = "fastersparql.client.max-query-get";
    public static final String CLIENT_CONN_RETRIES = "fastersparql.client.conn.retries";
    public static final String CLIENT_CONN_TIMEOUT_MS = "fastersparql.client.conn.timeout-ms";
    public static final String CLIENT_SO_TIMEOUT_MS = "fastersparql.client.so.timeout-ms";
    public static final String CLIENT_CONN_RETRY_WAIT_MS = "fastersparql.client.conn.retry.wait-ms";
    public static final String CLIENT_CONS_BATCH_MIN_WAIT_MS = "fastersparql.client.batch.min-wait-ms";
    public static final String OP_DISTINCT_CAPACITY = "fastersparql.op.distinct.capacity";
    public static final String OP_REDUCED_CAPACITY = "fastersparql.op.reduced.capacity";
    public static final String OP_DEDUP_CAPACITY = "fastersparql.op.dedup.capacity";
    public static final String OP_JOIN_REORDER = "fastersparql.op.join.reorder";
    public static final String OP_JOIN_REORDER_BIND = "fastersparql.op.join.reorder.bind";
    public static final String OP_JOIN_REORDER_HASH = "fastersparql.op.join.reorder.hash";
    public static final String OP_JOIN_REORDER_WCO = "fastersparql.op.join.reorder.wco";
    public static final String FED_ASK_POS_CAP = "fastersparql.fed.ask.pos.cap";
    public static final String FED_ASK_NEG_CAP = "fastersparql.fed.ask.neg.cap";

    /* --- --- --- default values --- --- --- */
    public static final int DEF_CLIENT_MAX_QUERY_GET = 1024;
    public static final int DEF_CLIENT_CONN_RETRIES = 3;
    public static final int DEF_CLIENT_CONN_TIMEOUT_MS = 0;
    public static final int DEF_CLIENT_SO_TIMEOUT_MS = 0;
    public static final int DEF_CLIENT_CONN_RETRY_WAIT_MS = 1000;
    public static final int DEF_CLIENT_CONS_BATCH_MIN_WAIT_MS = 10;
    public static final int DEF_OP_DISTINCT_CAPACITY = 1<<20; // 1 Mi rows --> 8MiB
    public static final int DEF_OP_REDUCED_CAPACITY = 1<<16; // 64 Ki rows --> 512KiB
    public static final int DEF_OP_DEDUP_CAPACITY = 1<<8; // 256 rows --> 2KiB
    public static final int DEF_FED_ASK_POS_CAP = 1<<14;
    public static final int DEF_FED_ASK_NEG_CAP = 1<<12;

    /* --- --- --- internal use --- --- --- */

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

    /* --- --- --- management --- --- --- */

    /**
     * Drops all cached property values, causing properties to be re-read from
     * {@link System#getProperty(String)} and {@link System#getenv(String)}.
     */
    public static void refresh() {
        PROP_CACHE.clear();
    }

    /* --- --- --- accessors --- --- --- */

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

    /**
     * When a fixed-capacity DISTINCT implementation is requested without setting a capacity,
     * this is the default capacity.
     *
     * <p>A fixed capacity violates SPARQL semantics but is faster since it limits maximum
     * memory usage for storing the previous rows. The final result may contain
     * duplicates but queries with many results will complete faster and without raising an
     * {@link OutOfMemoryError}.</p>
     *
     * <p>The default is set in {@link FSProperties#DEF_OP_DISTINCT_CAPACITY} and
     * is 1Mi rows, which requires 8MiB in references (the rows themselves will
     * consume more memory).</p>
     *
     * @return a positive ({@code n > 0}) integer.
     */
    public static @Positive int distinctCapacity() {
        return readPositiveInt(OP_DISTINCT_CAPACITY, DEF_OP_DISTINCT_CAPACITY);
    }

    /**
     * fixed capacity of rows used to implement REDUCED.
     *
     * <p>Unlike a DISTINCT implemented with fixed-capacity, a query that uses REDUCED already
     * expects a low-effort de-duplication, thus the value for this should be smaller than
     * {@link FSProperties#distinctCapacity()} but larger than
     * {@link FSProperties#dedupCapacity()}.</p>
     *
     * <p>The default is 128*1024 rows ({@link FSProperties#DEF_OP_REDUCED_CAPACITY}),
     * which consumes 512KiB in row pointers.</p>
     *
     * @return a positive ({@code n > 0}) integer
     */
    public static @Positive int reducedCapacity() {
        return readPositiveInt(OP_REDUCED_CAPACITY, DEF_OP_REDUCED_CAPACITY);
    }

    /**
     * The capacity to use when de-duplicating intermediary results of a query.
     *
     * <p>Intermediary results deduplication arises in two scenarios:</p>
     *
     * <ul>
     *     <li>A DISTINCT is applied to the query and thus any intermediary step can
     *     be de-duplicated without changing the final result set.</li>
     *     <li>A UNION of the same query being submitted to distinct sources arises in federated
     *     query processing. If sources have duplicate data, dropping rows from one source
     *     that have already been output by another source will often be acceptable.</li>
     * </ul>
     *
     * <p>When unions are de-duplicated the capacity set here will be multiplied by the number
     * of sources. Thus, unions with many operands will have more capacity.</p>
     *
     * <p>The value for this property should be a {@code n} small to avoid turning an optimization
     * into a bottleneck. The default, 256 rows yields an array of 2KiB for the row
     * references, which can fit within a single page (linux uses 4KiB pages) and should not
     * cause large disturbances in the cache hit ratio for x86 systems.</p>
     */
    public static @Positive int dedupCapacity() {
        return readPositiveInt(OP_DEDUP_CAPACITY, DEF_OP_DEDUP_CAPACITY);
    }

    private static final class JoinReorderStrategyParser implements Parser<JoinReorderStrategy> {
        private static final JoinReorderStrategyParser INSTANCE = new JoinReorderStrategyParser();
        @Override
        public JoinReorderStrategy parse(String src,
                                         String val) throws IllegalArgumentException {
            JoinReorderStrategy s = JoinReorderStrategy.loadStrategy(val);
            if (s == null)
                throw new IllegalArgumentException("No JoinReorderStrategy found for "+src+"="+val);
            return s;
        }
    }

    /**
     * The {@link JoinReorderStrategy} to use for joins implemented with bind.
     *
     * <p>The default strategy is {@link AvoidCartesianJoinReorderStrategy}, which only tries to
     * avoid cartesian products, retaining the original operand order as much as possible
     * (i.e., minimal optimization).</p>
     *
     * @return a non-null {@link JoinReorderStrategy} implementation.
     */
    public static JoinReorderStrategy bindJoinReorder() {
        JoinReorderStrategyParser p = JoinReorderStrategyParser.INSTANCE;
        JoinReorderStrategy s = readProperty(OP_JOIN_REORDER_BIND, null, p);
        if (s == null)
            s = readProperty(OP_JOIN_REORDER, AvoidCartesianJoinReorderStrategy.INSTANCE, p);
        return s;
    }

    /**
     * Same as {@link FSProperties#bindJoinReorder()} but for hash-based joins.
     */
    public static JoinReorderStrategy hashJoinReorder() {
        JoinReorderStrategyParser p = JoinReorderStrategyParser.INSTANCE;
        JoinReorderStrategy s = readProperty(OP_JOIN_REORDER_HASH, null, p);
        if (s == null)
            s = readProperty(OP_JOIN_REORDER, AvoidCartesianJoinReorderStrategy.INSTANCE, p);
        return s;
    }

    /**
     * Same as {@link FSProperties#bindJoinReorder()} but for worst-case optimal joins.
     */
    public static JoinReorderStrategy wcoJoinReorder() {
        var p = JoinReorderStrategyParser.INSTANCE;
        JoinReorderStrategy s = readProperty(OP_JOIN_REORDER_WCO, null, p);
        if (s == null)
            s = readProperty(OP_JOIN_REORDER, NoneJoinReorderStrategy.INSTANCE, p);
        return s;
    }

    /** How many triple patterns {@link AskSelector} instances should remember
     *  for <strong>POSITIVE</strong> matches */
    public static int askPositiveCapacity() { return readPositiveInt(FED_ASK_POS_CAP, DEF_FED_ASK_POS_CAP); }

    /** How many triple patterns {@link AskSelector} instances should remember
     *  for <strong>NEGATIVE</strong> matches */
    public static int askNegativeCapacity() { return readPositiveInt(FED_ASK_NEG_CAP, DEF_FED_ASK_NEG_CAP); }
}
