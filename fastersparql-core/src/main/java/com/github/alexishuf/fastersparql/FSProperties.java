package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.async.GatheringEmitter;
import com.github.alexishuf.fastersparql.fed.selectors.AskSelector;
import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import com.github.alexishuf.fastersparql.store.StoreSparqlClient;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import jdk.jfr.Event;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class FSProperties {

    /* --- --- --- property names --- --- --- */
    public static final String USE_VECTORIZATION         = "fastersparql.vectorization";
    public static final String USE_UNSAFE                = "fastersparql.unsafe";
    public static final String CLIENT_MAX_QUERY_GET      = "fastersparql.client.max-query-get";
    public static final String CLIENT_CONN_RETRIES       = "fastersparql.client.conn.retries";
    public static final String CLIENT_CONN_TIMEOUT_MS    = "fastersparql.client.conn.timeout-ms";
    public static final String CLIENT_SO_TIMEOUT_MS      = "fastersparql.client.so.timeout-ms";
    public static final String CLIENT_CONN_RETRY_WAIT_MS = "fastersparql.client.conn.retry.wait-ms";
    public static final String BATCH_POOLED_MARK         = "fastersparql.batch.pooled.mark";
    public static final String BATCH_POOLED_TRACE        = "fastersparql.batch.pooled.trace";
    public static final String BATCH_JFR_ENABLED         = "fastersparql.batch.jfr.enabled";
    public static final String BATCH_MIN_SIZE            = "fastersparql.batch.min-size";
    public static final String BATCH_MIN_WAIT_US         = "fastersparql.batch.min-wait-us";
    public static final String BATCH_MAX_WAIT_US         = "fastersparql.batch.max-wait-us";
    public static final String BATCH_SELF_VALIDATE       = "fastersparql.batch.self-validate";
    public static final String WS_SERVER_BINDINGS        = "fastersparql.ws.server.bindings";
    public static final String BATCH_QUEUE_ROWS          = "fastersparql.batch.queue.rows";
    public static final String OP_DISTINCT_CAPACITY      = "fastersparql.op.distinct.capacity";
    public static final String OP_WEAKEN_DISTINCT        = "fastersparql.op.distinct.weaken";
    public static final String OP_REDUCED_BATCHES        = "fastersparql.op.reduced.batches";
    public static final String OP_CROSS_DEDUP            = "fastersparql.op.cross-dedup";
    public static final String OP_OPPORTUNISTIC_DEDUP    = "fastersparql.op.opportunistic-dedup";
    public static final String OP_JOIN_REORDER           = "fastersparql.op.join.reorder";
    public static final String OP_JOIN_REORDER_BIND      = "fastersparql.op.join.reorder.bind";
    public static final String OP_JOIN_REORDER_HASH      = "fastersparql.op.join.reorder.hash";
    public static final String OP_JOIN_REORDER_WCO       = "fastersparql.op.join.reorder.wco";
    public static final String FED_ASK_POS_CAP           = "fastersparql.fed.ask.pos.cap";
    public static final String FED_ASK_NEG_CAP           = "fastersparql.fed.ask.neg.cap";
    public static final String EMIT_REQ_CHUNK_BATCHES    = "fastersparql.emit.request.chunk.batches";
    public static final String EMIT_LOG_STATS            = "fastersparql.emit.log-stats";
    public static final String STORE_CLIENT_VALIDATE     = "fastersparql.store.client.validate";
    public static final String STORE_PREFER_IDS          = "fastersparql.store.prefer-ids";
    public static final String NETTY_EVLOOP_THREADS      = "io.netty.eventLoopThreads";

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
    public static final int     DEF_OP_REDUCED_BATCHES        = 256;
    public static final int     DEF_FED_ASK_POS_CAP           = 1<<14;
    public static final int     DEF_FED_ASK_NEG_CAP           = 1<<12;
    public static final int     DEF_NETTY_EVLOOP_THREADS      = 0;
    public static final int     DEF_EMIT_REQ_CHUNK_BATCHES    = 8;
    public static final boolean DEF_OP_WEAKEN_DISTINCT        = false;
    public static final boolean DEF_OP_CROSS_DEDUP            = true;
    public static final boolean DEF_OP_OPPORTUNISTIC_DEDUP    = true;
    public static final boolean DEF_EMIT_LOG_STATS            = false;
    public static final boolean DEF_STORE_CLIENT_VALIDATE     = false;
    public static final boolean DEF_STORE_PREFER_IDS          = true;
    public static final Boolean DEF_BATCH_POOLED_TRACE        = false;

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
    private static int CACHE_OP_REDUCED_BATCHES        = -1;
    private static int CACHE_FED_ASK_POS_CAP           = -1;
    private static int CACHE_FED_ASK_NEG_CAP           = -1;
    private static int CACHE_EMIT_REQ_CHUNK_BATCHES    = -1;
    private static int CACHE_NETTY_EVLOOP_THREADS      = -1;
    private static Boolean CACHE_OP_WEAKEN_DISTINCT     = null;
    private static Boolean CACHE_USE_VECTORIZATION      = null;
    private static Boolean CACHE_USE_UNSAFE             = null;
    private static Boolean CACHE_OP_CROSS_DEDUP         = null;
    private static Boolean CACHE_OP_OPPORTUNISTIC_DEDUP = null;
    private static Boolean CACHE_EMIT_LOG_STATS         = null;
    private static Boolean CACHE_STORE_CLIENT_VALIDATE  = null;
    private static Boolean CACHE_STORE_PREFER_IDS       = null;
    private static Boolean CACHE_BATCH_POOLED_MARK      = null;
    private static Boolean CACHE_BATCH_POOLED_TRACE     = null;
    private static Boolean CACHE_BATCH_JFR_ENABLED      = null;
    private static Batch.Validation CACHE_BATCH_SELF_VALIDATE   = null;
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

    @SuppressWarnings("SameParameterValue")
    protected static @NonNegative int readNonNegativeInteger(String propertyName, int defaultValue) {
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
        CACHE_OP_REDUCED_BATCHES        = -1;
        CACHE_FED_ASK_POS_CAP           = -1;
        CACHE_FED_ASK_NEG_CAP           = -1;
        CACHE_EMIT_REQ_CHUNK_BATCHES    = -1;
        CACHE_NETTY_EVLOOP_THREADS      = -1;
        CACHE_OP_WEAKEN_DISTINCT        = null;
        CACHE_USE_VECTORIZATION         = null;
        CACHE_USE_UNSAFE                = null;
        CACHE_OP_CROSS_DEDUP            = null;
        CACHE_EMIT_LOG_STATS            = null;
        CACHE_BATCH_POOLED_MARK         = null;
        CACHE_BATCH_POOLED_TRACE        = null;
        CACHE_BATCH_SELF_VALIDATE       = null;
        CACHE_OP_JOIN_REORDER           = null;
        CACHE_OP_JOIN_REORDER_BIND      = null;
        CACHE_OP_JOIN_REORDER_HASH      = null;
        CACHE_OP_JOIN_REORDER_WCO       = null;
        CACHE_STORE_CLIENT_VALIDATE     = null;
        CACHE_STORE_PREFER_IDS          = null;
    }

    /* --- --- --- accessors --- --- --- */

    /** Whether we are running in GraalVM JDK or in a GraalVM antive image. */
    @SuppressWarnings({"BooleanMethodIsAlwaysInverted", "SpellCheckingInspection"})
    public static boolean onGraal() {
        return System.getProperty("org.graalvm.home") != null
                || System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    }

    /**
     * Whether fastersparql code should use the incubating vectorization API. The
     * <strong>default</strong> is to not use if running in GraalVM (native or not) since it
     * (currently) does not implement the intrinsics, causing the vectorization API to
     * emulate vectorization which is worse than doing scalar loops.
     *
     * <p>Changing this setting might have no effect at runtime if relevant {@code static final}
     * fields that query this have already been initialized.</p>
     */
    public static boolean useVectorization() {
        Boolean v = CACHE_USE_VECTORIZATION;
        if (v == null)
            CACHE_USE_VECTORIZATION = v = readBoolean(USE_VECTORIZATION, !onGraal());
        return v == Boolean.TRUE;
    }

    /**
     * Whether fastersparql code should use {@link sun.misc.Unsafe}. Changed to this property
     * might not have any effect if set at runtime due to {@code static final} fields having
     * already been initialized. <strong>The default is</strong> to use unsafe if available
     * and not running in GraalVM (native or not)
     *
     * <p>This setting will not affect netty. Use {@code io.netty.noUnsafe=true} to forbid Netty from using {@link sun.misc.Unsafe} even if available.</p>
     */
    public static boolean useUnsafe() {
        Boolean v = CACHE_USE_UNSAFE;
        if (v == null)
            CACHE_USE_UNSAFE = v = readBoolean(USE_UNSAFE, !onGraal());
        return v == Boolean.TRUE;
    }

    /**
     * If {@code true} {@link Emitter}s will log statistics when terminated if there is no
     * {@link Emitter#rebindAcquire()} in effect. This is disabled by default and should only be
     * enabled for debugging.
     *
     * <p>Note that {@link Emitter} implementations query this via a {@code static final}
     * field, so that code pertaining to this logging (including the check) is eliminated by the
     * JIT compiler. Therefore changes at runtime may be ignored and this should be set with
     * {@code -D} on the command-line. </p>
     *
     * @return {@code true} if {@link Emitter} should log statistics upon termination.
     */
    public static boolean emitLogStats() {
        Boolean v = CACHE_EMIT_LOG_STATS;
        if (v == null)
            CACHE_EMIT_LOG_STATS = v = readBoolean(EMIT_LOG_STATS, DEF_EMIT_LOG_STATS);
        return v;
    }

    /**
     * {@link GatheringEmitter} fragments {@link Emitter#request(long)} into fragments which
     * are sized to roughly correspond to {@code emitReqChunkBatches} batches. This fragmentation
     * avoids the {@link GatheringEmitter} downstream receiver from being overwhelmed due to
     * having its request multiplied by the number of upstreams of the {@link GatheringEmitter}.
     *
     * <p>The default value is {@link #DEF_EMIT_REQ_CHUNK_BATCHES} and this can be changed at
     * runtime by setting the {@link #EMIT_REQ_CHUNK_BATCHES} property (and calling
     * {@link #refresh()}).</p>
     *
     * @return the number of batches that a request chunk should comprise.
     */
    public static int emitReqChunkBatches() {
        int v = CACHE_EMIT_REQ_CHUNK_BATCHES;
        if (v == -1) {
            v = readPositiveInt(EMIT_REQ_CHUNK_BATCHES, DEF_EMIT_REQ_CHUNK_BATCHES);
            CACHE_EMIT_REQ_CHUNK_BATCHES = v;
        }
        return v;
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
     * Whether stack traces should be collected every time a {@link Batch} enter or leaves a pool.
     *
     * <p>The default is false, since the overhead generated by their collection makes tests
     * too slow. This should be enabled only once a pooling issue has been found via
     * {@link #batchPooledMark()}.</p>
     *
     * <p>For performance reasons (i.e., enabling dead-code elimination), this property is read
     * into a {@code static final} field when relevant classes are loaded. Thus changing the
     * property and calling {@link #refresh()} might have no effect on the actual behavior.</p>
     *
     * @return whether stack traces should be collected when a batch enters or leaves a pool.
     */
    public static boolean batchPooledTrace() {
        Boolean v = CACHE_BATCH_POOLED_TRACE;
        if (v == null)
            CACHE_BATCH_POOLED_TRACE = v = readBoolean(BATCH_POOLED_TRACE, DEF_BATCH_POOLED_TRACE);
        return v;
    }

    /**
     * Whether batches should be mark as pooled or not. Marking an already pooled batch
     * as pooled or an already unpooled batch as unpooled will raise a {@link RuntimeException}.
     *
     * <p>By default, this is enabled if assertions are enabled ({@code -ea}) JVM flag.</p>
     *
     * <p>For performance reasons (i.e., enabling dead-code elimination), this property is read
     * into a {@code static final} field when relevant classes are loaded. Thus changing the
     * property and calling {@link #refresh()} might have no effect on the actual behavior.</p>
     *
     * @return whether batches should be marked as pooled or unpooled and doubly pooling/unpooling
     *         should raise an exception.
     */
    public static boolean batchPooledMark() {
        Boolean v = CACHE_BATCH_POOLED_MARK;
        if (v == null) {
            boolean def = FSProperties.class.desiredAssertionStatus();
            CACHE_BATCH_POOLED_MARK = v = readBoolean(BATCH_POOLED_MARK, def);
        }
        return v;
    }

    /**
     * Whether Batch custom events may be published to Java Flight Recorder. Note that this
     * property does not imply that JFR will be enabled or that the custom events themselves will
     * be enabled in the JFR recording configuration.
     *
     * <p>By default, this is enabled if assertions are enabled ({@code -ea}) JVM flag.</p>
     *
     * <p>For performance reasons (i.e., enabling dead-code elimination), this property is read
     * into a {@code static final} field when relevant classes are loaded. Thus changing the
     * property and calling {@link #refresh()} might have no effect on the actual behavior.</p>
     *
     * @return whether batch-related custom events should be filled and {@link Event#commit()}ed
     *         to the JFR.
     */
    public static boolean batchJFREnabled() {
        Boolean v = CACHE_BATCH_JFR_ENABLED;
        if (v == null) {
            boolean def = FSProperties.class.desiredAssertionStatus();
            CACHE_BATCH_JFR_ENABLED = v = readBoolean(BATCH_JFR_ENABLED, def);
        }
        return v;
    }

    private static final Batch.Validation[] SELF_VALIDATIONS_VALUES = Batch.Validation.values();

    /**
     * Whether {@link Batch} implementations should perform self-tests to ensure
     * that implementation-specific invariants are preserved after every mutation. Such checks
     * may be expensive. <strong>The default is to enable this only if assertions are
     * enabled</strong> ({@code -ea} JVM flag).
     *
     * <p>Changing this property at runtime, after may have no effect if the classes implementing
     * {@link Batch} have already been loaded.</p>
     *
     * @return {@code true} iff {@link Batch} implementations should self-test after every mutation.
     */
    public static Batch.Validation batchSelfValidate() {
        Batch.Validation v = CACHE_BATCH_SELF_VALIDATE;
        if (v == null) {
            Batch.Validation def = FSProperties.class.desiredAssertionStatus()
                    ? Batch.Validation.EXPENSIVE : Batch.Validation.NONE;
            v = readEnum(BATCH_SELF_VALIDATE, SELF_VALIDATIONS_VALUES, def);
            CACHE_BATCH_SELF_VALIDATE = v;
        }
        return v;
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
     * Whether {@code DISTINCT} should be transparently evaluated as {@code REDUCED}.
     *
     * <p>This does not apply to {@code DISTINCT} clauses in queries sent to endpoints where
     * {@link SparqlClient#isLocalInProcess()}{@code == false}.</p>
     *
     * <p>The default is {@link #DEF_OP_WEAKEN_DISTINCT} and this can be modified by setting
     * the {@link #OP_WEAKEN_DISTINCT} java property at startup. Setting the property after
     * startup may have no effect since the property value is loaded into a
     * {@code public static final} field.</p>
     *
     * @return whether {@code DISTINCT} should be evaluated as {@code REDUCED}.
     */
    public static boolean weakenDistinct() {
        Boolean v = CACHE_OP_WEAKEN_DISTINCT;
        if (v == null)
            CACHE_OP_WEAKEN_DISTINCT = v = readBoolean(OP_WEAKEN_DISTINCT, DEF_OP_WEAKEN_DISTINCT);
        return Boolean.TRUE.equals(v);
    }

    /**
     * fixed capacity of rows used to implement {@code REDUCED}.
     *
     * <p>Since {@code REDUCED} allow for both duplicate removal as well as non-removal, a
     * de-duplication is implemented in constant-time (even under heavy hash collision)
     * using a storage equivalent of this many <strong>batches</strong>. Note that unlike
     * {@link #distinctCapacity()}, this is expressed in abtches since de-duplication cost
     * is more important than de-duplication correctness.</p>
     *
     * <p>The default is {@link #DEF_OP_REDUCED_BATCHES} batches and can be configured via the
     * {@link #OP_REDUCED_BATCHES} java property. Due to pooling, the old capacity may still
     * be occasionally used if the property is changed at runtime after some queries using
     * {@code REDUCED} have already been processed.</p>
     *
     * @return a positive ({@code n > 0}) integer
     */
    public static @NonNegative int reducedBatches() {
        int i = CACHE_OP_REDUCED_BATCHES;
        if (i < 0)
            CACHE_OP_REDUCED_BATCHES = i =  readPositiveInt(OP_REDUCED_BATCHES, DEF_OP_REDUCED_BATCHES);
        return i;
    }

    /**
     * Whether the results of unions of joins that all share the same left-side source and share
     * the same algebra for the right side (but are directed to distinct endpoints) should be
     * cross-source de-duplicated.
     *
     * <p>In cross-source deduplication, if the same row is emitted twice by the same source, the
     * second occurrence will not be eliminated, but if a row is emitted once by a source and again
     * by another, the second occurrence may be dropped if it was possible to observe the previous
     * occurrence in the limited, moving history.</p>
     *
     * <p>The <strong>default</strong> for this is {@code true} ({@link #DEF_OP_CROSS_DEDUP}.
     * The java property name is {@link #OP_CROSS_DEDUP}</p>
     *
     * @return Whether cross-source dedup is enabled.
     */
    public static boolean crossDedup() {
        Boolean v = CACHE_OP_CROSS_DEDUP;
        if (v == null)
            CACHE_OP_CROSS_DEDUP = v = readBoolean(OP_CROSS_DEDUP, DEF_OP_CROSS_DEDUP);
        return Boolean.TRUE.equals(v);
    }

    /**
     * Whether results should be opportunistically de-duplicated as early as possible during
     * execution. I.e., if a query has a globally applied DISTINCT or REDUCED, and doing so would
     * not change semantics, leaf nodes of the execution will employ low-effort deduplication in
     * order to reduce the number of rows to be evaluated by the execution node that implements
     * the DISTINCT/REDUCED clause requested by the query.
     *
     * <p>The <strong>default</strong> value is true ({@link #DEF_OP_OPPORTUNISTIC_DEDUP}). The
     * java corresponding property name is {@link #OP_OPPORTUNISTIC_DEDUP}.</p>
     *
     * @return 1 of opportunistic deduplication is enabled, 0 otherwise.
     */
    public static boolean opportunisticDedup() {
        Boolean v = CACHE_OP_OPPORTUNISTIC_DEDUP;
        if (v == null)
            CACHE_OP_OPPORTUNISTIC_DEDUP = v = readBoolean(OP_OPPORTUNISTIC_DEDUP, DEF_OP_OPPORTUNISTIC_DEDUP);
        return Boolean.TRUE.equals(v);
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

    /**
     * Whether a BGP wholly assigned to a single {@link StoreSparqlClient} should be executed
     * using the native {@link StoreBatch} even if another batch type was requested.
     *
     * <p>The default is {@link #DEF_STORE_PREFER_IDS} and this can be overridden using the
     * {@link #STORE_PREFER_IDS} property. However, changes to the property will only have an
     * effect if the change occurs before the {@code static final} fields whose initialization
     * calls this method are initialized</p>
     */
    public static boolean storePreferIds() {
        Boolean v = CACHE_STORE_PREFER_IDS;
        if (v == null)
            CACHE_STORE_PREFER_IDS = v = readBoolean(STORE_PREFER_IDS, DEF_STORE_PREFER_IDS);
        return v;
    }

    /**
     * How many threads a netty event loop should have by default. This is controlled by the
     * same property used by netty itself ({@code io.netty.eventLoopThreads}). If the property
     * is unset the default will be {@link Runtime#availableProcessors()} instead of the actual
     * netty default that would be double that. If the property is set to 0, the netty default
     * behavior will remain.
     *
     * @return How many threads should a netty {@code EventLoopGroup} have.
     */
    public static int nettyEventLoopThreads() {
        int i = CACHE_NETTY_EVLOOP_THREADS;
        if (i < 0) {
            i = readNonNegativeInteger(NETTY_EVLOOP_THREADS, Integer.MAX_VALUE);
            if (i == Integer.MAX_VALUE)
                i = Runtime.getRuntime().availableProcessors();
            CACHE_NETTY_EVLOOP_THREADS = i;
        }
        return i;
    }
}
