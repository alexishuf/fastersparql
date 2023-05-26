package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.fed.selectors.AskSelector;
import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import com.github.alexishuf.fastersparql.store.StoreSparqlClient;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class FSProperties {

    /* --- --- --- property names --- --- --- */
    public static final String CLIENT_MAX_QUERY_GET      = "fastersparql.client.max-query-get";
    public static final String CLIENT_CONN_RETRIES       = "fastersparql.client.conn.retries";
    public static final String CLIENT_CONN_TIMEOUT_MS    = "fastersparql.client.conn.timeout-ms";
    public static final String CLIENT_SO_TIMEOUT_MS      = "fastersparql.client.so.timeout-ms";
    public static final String CLIENT_CONN_RETRY_WAIT_MS = "fastersparql.client.conn.retry.wait-ms";
    public static final String BATCH_MIN_SIZE            = "fastersparql.batch.min-size";
    public static final String BATCH_MIN_WAIT_US         = "fastersparql.batch.min-wait-us";
    public static final String BATCH_MAX_WAIT_US         = "fastersparql.batch.max-wait-us";
    public static final String WS_SERVER_BINDINGS        = "fastersparql.ws.server.bindings";
    public static final String BATCH_QUEUE_ROWS          = "fastersparql.batch.queue.rows";
    public static final String OP_DISTINCT_CAPACITY      = "fastersparql.op.distinct.capacity";
    public static final String OP_REDUCED_CAPACITY       = "fastersparql.op.reduced.capacity";
    public static final String OP_DEDUP_CAPACITY         = "fastersparql.op.dedup.capacity";
    public static final String OP_CROSS_DEDUP_CAPACITY   = "fastersparql.op.cross-source-dedup";
    public static final String OP_JOIN_REORDER           = "fastersparql.op.join.reorder";
    public static final String OP_JOIN_REORDER_BIND      = "fastersparql.op.join.reorder.bind";
    public static final String OP_JOIN_REORDER_HASH      = "fastersparql.op.join.reorder.hash";
    public static final String OP_JOIN_REORDER_WCO       = "fastersparql.op.join.reorder.wco";
    public static final String FED_ASK_POS_CAP           = "fastersparql.fed.ask.pos.cap";
    public static final String FED_ASK_NEG_CAP           = "fastersparql.fed.ask.neg.cap";
    public static final String STORE_CLIENT_VALIDATE     = "fastersparql.store.client.validate";

    /* --- --- --- default values --- --- --- */
    public static final int     DEF_CLIENT_MAX_QUERY_GET      = 1024;
    public static final int     DEF_CLIENT_CONN_RETRIES       = 3;
    public static final int     DEF_CLIENT_CONN_TIMEOUT_MS    = 0;
    public static final int     DEF_CLIENT_SO_TIMEOUT_MS      = 0;
    public static final int     DEF_CLIENT_CONN_RETRY_WAIT_MS = 1000;
    public static final int     DEF_BATCH_MIN_SIZE            = BIt.PREFERRED_MIN_BATCH;
    public static final int     DEF_BATCH_MIN_WAIT_US         = BIt.QUICK_MIN_WAIT_NS/1_000;
    public static final int     DEF_BATCH_MAX_WAIT_US         = 2*BIt.QUICK_MIN_WAIT_NS/1_000;
    public static final int     DEF_WS_SERVER_BINDINGS        = 256;
    public static final int     DEF_BATCH_QUEUE_ROWS          = 1<<15;
    public static final int     DEF_OP_DISTINCT_CAPACITY      = 1<<20; // 1 Mi rows --> 8MiB
    public static final int     DEF_OP_REDUCED_CAPACITY       = 1<<16; // 64 Ki rows --> 512KiB
    public static final int     DEF_OP_DEDUP_CAPACITY         = 1<<8; // 256 rows --> 2KiB
    public static final int     DEF_OP_CROSS_DEDUP_CAPACITY   = 1<<8;
    public static final int     DEF_FED_ASK_POS_CAP           = 1<<14;
    public static final int     DEF_FED_ASK_NEG_CAP           = 1<<12;
    public static final boolean DEF_STORE_CLIENT_VALIDATE     = false;

    /* --- --- --- cached values --- --- --- */
    private static int CACHE_CLIENT_MAX_QUERY_GET      = -1;
    private static int CACHE_CLIENT_CONN_RETRIES       = -1;
    private static int CACHE_CLIENT_CONN_TIMEOUT_MS    = -1;
    private static int CACHE_CLIENT_SO_TIMEOUT_MS      = -1;
    private static int CACHE_CLIENT_CONN_RETRY_WAIT_MS = -1;
    private static int CACHE_BATCH_MIN_SIZE            = -1;
    private static int CACHE_BATCH_MIN_WAIT_US         = -1;
    private static int CACHE_BATCH_MAX_WAIT_US         = -1;
    private static int CACHE_WS_SERVER_BINDINGS        = -1;
    private static int CACHE_BATCH_QUEUE_ROWS          = -1;
    private static int CACHE_OP_DISTINCT_CAPACITY      = -1;
    private static int CACHE_OP_REDUCED_CAPACITY       = -1;
    private static int CACHE_OP_DEDUP_CAPACITY         = -1;
    private static int CACHE_OP_CROSS_DEDUP_CAPACITY   = -1;
    private static int CACHE_FED_ASK_POS_CAP           = -1;
    private static int CACHE_FED_ASK_NEG_CAP           = -1;
    private static Boolean CACHE_STORE_CLIENT_VALIDATE = null;
    private static JoinReorderStrategy CACHE_OP_JOIN_REORDER      = null;
    private static JoinReorderStrategy CACHE_OP_JOIN_REORDER_BIND = null;
    private static JoinReorderStrategy CACHE_OP_JOIN_REORDER_HASH = null;
    private static JoinReorderStrategy CACHE_OP_JOIN_REORDER_WCO  = null;

    /* --- --- --- internal use --- --- --- */

    protected interface Parser<T> {
        T parse(String source, String value) throws IllegalArgumentException;
    }

    protected static <T> T readProperty(String propertyName, T defaultValue,
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


    private static final Pattern BOOL_RX =
            Pattern.compile("(?i)\\s*(?:(t|true|1|y|yes)|(f|false|0|n|no))\\s*");
    protected static boolean readBoolean(String propertyName, boolean defaultValue) {
        return readProperty(propertyName, defaultValue, (src, val) -> {
            Matcher m = BOOL_RX.matcher(val);
            if (!m.matches())
                throw new IllegalArgumentException(src+"="+val+" is not a boolean");
            return m.group(1) != null;
        });
    }

    protected static @Positive int readNonNegativeInteger(String propertyName, int defaultValue) {
        return readProperty(propertyName, defaultValue, (src, val) -> {
            int i = -1;
            try { i = Integer.parseInt(val); } catch (NumberFormatException ignored) {}
            if (i < 0)
                throw new IllegalArgumentException(src+"="+val+" is negative");
            return i;
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

    protected static <E extends Enum<E>> E readEnum(String propertyName, E[] values, E defaultValue) {
        return readProperty(propertyName, defaultValue, (src, val) -> {
            val = val.trim();
            for (E e : values) {
                if (e.name().equalsIgnoreCase(val))
                    return e;
            }
            throw new IllegalArgumentException(src+"="+val+" is not in"+ Arrays.toString(values));
        });
    }

    /* --- --- --- management --- --- --- */

    /**
     * Drops all cached property values, causing properties to be re-read from
     * {@link System#getProperty(String)} and {@link System#getenv(String)}.
     */
    public static void refresh() {
        CACHE_CLIENT_MAX_QUERY_GET      = -1;
        CACHE_CLIENT_CONN_RETRIES       = -1;
        CACHE_CLIENT_CONN_TIMEOUT_MS    = -1;
        CACHE_CLIENT_SO_TIMEOUT_MS      = -1;
        CACHE_CLIENT_CONN_RETRY_WAIT_MS = -1;
        CACHE_BATCH_MIN_SIZE            = -1;
        CACHE_BATCH_MIN_WAIT_US         = -1;
        CACHE_BATCH_MAX_WAIT_US         = -1;
        CACHE_WS_SERVER_BINDINGS        = -1;
        CACHE_BATCH_QUEUE_ROWS          = -1;
        CACHE_OP_DISTINCT_CAPACITY      = -1;
        CACHE_OP_REDUCED_CAPACITY       = -1;
        CACHE_OP_DEDUP_CAPACITY         = -1;
        CACHE_OP_CROSS_DEDUP_CAPACITY   = -1;
        CACHE_FED_ASK_POS_CAP           = -1;
        CACHE_FED_ASK_NEG_CAP           = -1;
        CACHE_OP_JOIN_REORDER           = null;
        CACHE_OP_JOIN_REORDER_BIND      = null;
        CACHE_OP_JOIN_REORDER_HASH      = null;
        CACHE_OP_JOIN_REORDER_WCO       = null;
        CACHE_STORE_CLIENT_VALIDATE     = null;
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
        int i = CACHE_CLIENT_MAX_QUERY_GET;
        if (i < 0)
            CACHE_CLIENT_MAX_QUERY_GET = i = readPositiveInt(CLIENT_MAX_QUERY_GET, DEF_CLIENT_MAX_QUERY_GET);
        return i;
    }

    /**
     * How many times a {@link SparqlClient} should retry opening a connection to an endpoint if
     * the connection was actively refused or timed out.
     *
     * @return A number {@code >= 0} indicating how many retries should be made. If zero,
     *         there will be only the initial connection attempt.
     */
    public static @NonNegative int maxRetries() {
        int i = CACHE_CLIENT_CONN_RETRIES;
        if (i < 0)
            CACHE_CLIENT_CONN_RETRIES = i = readPositiveInt(CLIENT_CONN_RETRIES, DEF_CLIENT_CONN_RETRIES);
        return i;
    }

    /**
     * Timeout, in millis, for establishing a TCP connection.
     *
     * @return Either {@code 0}, delegating the choice the underlying OS, or a value {@code > 0}
     *         with the number of milliseconds after which TCP connections that failed to complete
     *         the handshake are to be considered failed.
     */
    public static @NonNegative int connectTimeoutMs() {
        int i = CACHE_CLIENT_CONN_TIMEOUT_MS;
        if (i < 0)
            CACHE_CLIENT_CONN_TIMEOUT_MS = i = readPositiveInt(CLIENT_CONN_TIMEOUT_MS, DEF_CLIENT_CONN_TIMEOUT_MS);
        return i;
    }

    /**
     * Timeout in milliseconds for all socket operations other than connect.
     *
     * @return Either {@code 0}, delegating the choice to the underlying OS, or a value {@code >0}
     *         with the timeout in milliseconds to be set for non-connect socket operations.
     */
    public static @NonNegative int soTimeoutMs() {
        int i = CACHE_CLIENT_SO_TIMEOUT_MS;
        if (i < 0)
            CACHE_CLIENT_SO_TIMEOUT_MS = i = readPositiveInt(CLIENT_SO_TIMEOUT_MS, DEF_CLIENT_SO_TIMEOUT_MS);
        return i;
    }

    /**
     * How much time to wait before each of the connection retries
     * ({@link FSProperties#maxRetries()}).
     *
     * @param timeUnit the desired time unit of the time window.
     * @return a non-negative number of timeunits to wait before each retry.
     */
    public static @NonNegative long retryWait(TimeUnit timeUnit) {
        int ms = CACHE_CLIENT_CONN_RETRY_WAIT_MS;
        if (ms < 0)
            CACHE_CLIENT_CONN_RETRY_WAIT_MS = ms = readPositiveInt(CLIENT_CONN_RETRY_WAIT_MS, DEF_CLIENT_CONN_RETRY_WAIT_MS);
        return timeUnit.convert(ms, TimeUnit.MILLISECONDS);
    }

    /**
     * The default value for {@link BIt#minBatch(int)} of iterators that knowingly receive data
     * from an asynchronous source.
     *
     * @return a size {@code >= 1}.
     */
    public static @Positive int batchMinSize() {
        int i = CACHE_BATCH_MIN_SIZE;
        if (i <= 0)
            CACHE_BATCH_MIN_SIZE = i = readPositiveInt(BATCH_MIN_SIZE, DEF_BATCH_MIN_SIZE);
        return i;
    }

    /**
     * The default value for {@link BIt#minWait(long, TimeUnit)} in iterators that knowingly
     * receive data from an asynchronous source and not from a intermediary processing step.
     *
     * @param timeUnit The desired unit of the duration value.
     * @return a non-negative value to be passed to {@link BIt#minWait(long, TimeUnit)}
     */
    public static @NonNegative long batchMinWait(TimeUnit timeUnit) {
        int ms = CACHE_BATCH_MIN_WAIT_US;
        if (ms < 0)
            CACHE_BATCH_MIN_WAIT_US = ms = readPositiveInt(BATCH_MIN_WAIT_US, DEF_BATCH_MIN_WAIT_US);
        return timeUnit.convert(ms, TimeUnit.MICROSECONDS);
    }


    /**
     * Analogous to {@link FSProperties#batchMinWait(TimeUnit)}, but refers to
     * {@link BIt#maxWait(long, TimeUnit)}
     *
     * @param timeUnit desired time unit of the wait duration
     * @return a non-negative value to be passed to {@link BIt#maxWait(long, TimeUnit)}
     */
    public static @NonNegative long batchMaxWait(TimeUnit timeUnit) {
        int ms = CACHE_BATCH_MAX_WAIT_US;
        if (ms < 0)
            CACHE_BATCH_MAX_WAIT_US = ms = readPositiveInt(BATCH_MAX_WAIT_US, DEF_BATCH_MAX_WAIT_US);
        return timeUnit.convert(ms, TimeUnit.MICROSECONDS);
    }

    /**
     * The default number of bindings that a server will initially request from the client
     * in a {@code !bind} operation. As the bindings are processed, the server will send new,
     * smaller requests to the client aiming to keep at most this number of bindings ready at
     * the server side.
     *
     * @return a positive number of rows
     */
    public static @Positive int wsServerBindings() {
        int i = CACHE_WS_SERVER_BINDINGS;
        if (i <= 0) CACHE_WS_SERVER_BINDINGS = i = readPositiveInt(WS_SERVER_BINDINGS, DEF_WS_SERVER_BINDINGS);
        return i;
    }

    /**
     * The default maximum number of rows (distributed among all queued batches) that a
     * queue-backed {@link BIt} may hold by default.
     *
     * @return a positive number of rows. {@link Integer#MAX_VALUE} represents no upper bound
     */
    public static @Positive int queueMaxRows() {
        int i = CACHE_BATCH_QUEUE_ROWS;
        if (i <= 0)
            CACHE_BATCH_QUEUE_ROWS = i = readPositiveInt(BATCH_QUEUE_ROWS, DEF_BATCH_QUEUE_ROWS);
        return i;
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
    public static @NonNegative int distinctCapacity() {
        int i = CACHE_OP_DISTINCT_CAPACITY;
        if (i < 0)
            CACHE_OP_DISTINCT_CAPACITY = i = readPositiveInt(OP_DISTINCT_CAPACITY, DEF_OP_DISTINCT_CAPACITY);
        return i;
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
    public static @NonNegative int reducedCapacity() {
        int i = CACHE_OP_REDUCED_CAPACITY;
        if (i < 0)
            CACHE_OP_REDUCED_CAPACITY = i =  readPositiveInt(OP_REDUCED_CAPACITY, DEF_OP_REDUCED_CAPACITY);
        return i;
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
    public static @NonNegative int dedupCapacity() {
        int i = CACHE_OP_DEDUP_CAPACITY;
        if (i < 0)
            CACHE_OP_DEDUP_CAPACITY = i = readPositiveInt(OP_DEDUP_CAPACITY, DEF_OP_DEDUP_CAPACITY);
        return i;
    }

    public static int crossDedupCapacity() {
        int i = CACHE_OP_CROSS_DEDUP_CAPACITY;
        if (i < 0)
            CACHE_OP_CROSS_DEDUP_CAPACITY = i = readNonNegativeInteger(OP_CROSS_DEDUP_CAPACITY, DEF_OP_CROSS_DEDUP_CAPACITY);
        return i;
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

    private static JoinReorderStrategy joinReorder() {
        JoinReorderStrategy s = CACHE_OP_JOIN_REORDER;
        if (s == null)
            CACHE_OP_JOIN_REORDER = s = readProperty(OP_JOIN_REORDER, AvoidCartesianJoinReorderStrategy.INSTANCE, JoinReorderStrategyParser.INSTANCE);
        return s;
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
        JoinReorderStrategy s = CACHE_OP_JOIN_REORDER_BIND;
        if (s == null) {
            JoinReorderStrategyParser p = JoinReorderStrategyParser.INSTANCE;
            s = readProperty(OP_JOIN_REORDER_BIND, null, p);
            if (s == null) s = joinReorder();
            CACHE_OP_JOIN_REORDER_BIND = s;
        }
        return s;
    }

    /**
     * Same as {@link FSProperties#bindJoinReorder()} but for hash-based joins.
     */
    public static JoinReorderStrategy hashJoinReorder() {
        JoinReorderStrategy s = CACHE_OP_JOIN_REORDER_HASH;
        if (s == null) {
            s = readProperty(OP_JOIN_REORDER_HASH, null, JoinReorderStrategyParser.INSTANCE);
            if (s == null)
                s = joinReorder();
            CACHE_OP_JOIN_REORDER_HASH = s;
        }
        return s;
    }

    /**
     * Same as {@link FSProperties#bindJoinReorder()} but for worst-case optimal joins.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static JoinReorderStrategy wcoJoinReorder() {
        var s = CACHE_OP_JOIN_REORDER_WCO;
        if (s == null) {
            var p = JoinReorderStrategyParser.INSTANCE;
            s = readProperty(OP_JOIN_REORDER_WCO, null, p);
            if (s == null)
                s = joinReorder();
            CACHE_OP_JOIN_REORDER_WCO = s;
        }
        return s;
    }

    /** How many triple patterns {@link AskSelector} instances should remember
     *  for <strong>POSITIVE</strong> matches */
    public static @Positive int askPositiveCapacity() {
        int i = CACHE_FED_ASK_POS_CAP;
        if (i <= 0)
            CACHE_FED_ASK_POS_CAP = i = readPositiveInt(FED_ASK_POS_CAP, DEF_FED_ASK_POS_CAP);
        return i;
    }

    /** How many triple patterns {@link AskSelector} instances should remember
     *  for <strong>NEGATIVE</strong> matches */
    public static @Positive int askNegativeCapacity() {
        int i = CACHE_FED_ASK_NEG_CAP;
        if (i <= 0)
            CACHE_FED_ASK_NEG_CAP = i = readPositiveInt(FED_ASK_NEG_CAP, DEF_FED_ASK_NEG_CAP);
        return i;
    }

    /** Whether {@link StoreSparqlClient} should validate indexes when loading. */
    public static boolean storeClientValidate() {
        Boolean v = CACHE_STORE_CLIENT_VALIDATE;
        if (v == null)
            CACHE_STORE_CLIENT_VALIDATE = v = readBoolean(STORE_CLIENT_VALIDATE, DEF_STORE_CLIENT_VALIDATE);
        return v;
    }
}
