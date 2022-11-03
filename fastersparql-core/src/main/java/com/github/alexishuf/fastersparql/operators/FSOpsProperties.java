package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.util.FSProperties;
import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.NoneJoinReorderStrategy;
import org.checkerframework.checker.index.qual.Positive;

@SuppressWarnings("UnusedReturnValue")
public class FSOpsProperties extends FSProperties {

    /* --- --- --- property names --- --- --- */
    public static final String OP_DISTINCT_CAPACITY = "fastersparql.op.distinct.capacity";
    public static final String OP_REDUCED_CAPACITY = "fastersparql.op.reduced.capacity";
    public static final String OP_DEDUP_CAPACITY = "fastersparql.op.dedup.capacity";
    public static final String OP_JOIN_REORDER = "fastersparql.op.join.reorder";
    public static final String OP_JOIN_REORDER_BIND = "fastersparql.op.join.reorder.bind";
    public static final String OP_JOIN_REORDER_HASH = "fastersparql.op.join.reorder.hash";
    public static final String OP_JOIN_REORDER_WCO = "fastersparql.op.join.reorder.wco";

    /* --- --- --- default values --- --- --- */
    public static final int DEF_OP_DISTINCT_CAPACITY = 1<<20; // 1 Mi rows --> 8MiB
    public static final int DEF_OP_REDUCED_CAPACITY = 1<<16; // 64 Ki rows --> 512KiB
    public static final int DEF_OP_DEDUP_CAPACITY = 1<<8; // 256 rows --> 2KiB


    /* --- --- --- accessors --- --- --- */

    /**
     * When a fixed-capacity DISTINCT implementation is requested without setting a capacity,
     * this is the default capacity.
     *
     * <p>A fixed capacity violates SPARQL semantics but is faster since it limits maximum
     * memory usage for storing the previous rows. The final result may contain
     * duplicates but queries with many results will complete faster and without raising an
     * {@link OutOfMemoryError}.</p>
     *
     * <p>The default is set in {@link FSOpsProperties#DEF_OP_DISTINCT_CAPACITY} and
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
     * {@link FSOpsProperties#distinctCapacity()} but larger than
     * {@link FSOpsProperties#dedupCapacity()}.</p>
     *
     * <p>The default is 128*1024 rows ({@link FSOpsProperties#DEF_OP_REDUCED_CAPACITY}),
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
     * Same as {@link FSOpsProperties#bindJoinReorder()} but for hash-based joins.
     */
    public static JoinReorderStrategy hashJoinReorder() {
        JoinReorderStrategyParser p = JoinReorderStrategyParser.INSTANCE;
        JoinReorderStrategy s = readProperty(OP_JOIN_REORDER_HASH, null, p);
        if (s == null)
            s = readProperty(OP_JOIN_REORDER, AvoidCartesianJoinReorderStrategy.INSTANCE, p);
        return s;
    }

    /**
     * Same as {@link FSOpsProperties#bindJoinReorder()} but for worst-case optimal joins.
     */
    public static JoinReorderStrategy wcoJoinReorder() {
        var p = JoinReorderStrategyParser.INSTANCE;
        JoinReorderStrategy s = readProperty(OP_JOIN_REORDER_WCO, null, p);
        if (s == null)
            s = readProperty(OP_JOIN_REORDER, NoneJoinReorderStrategy.INSTANCE, p);
        return s;
    }
}
