package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties;
import com.github.alexishuf.fastersparql.operators.expressions.ExprEvaluatorCompilerProvider;
import com.github.alexishuf.fastersparql.operators.reorder.AvoidCartesianJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.NullJoinReorderStrategy;
import org.checkerframework.checker.index.qual.Positive;

@SuppressWarnings("UnusedReturnValue")
public class FasterSparqlOpProperties extends FasterSparqlProperties {

    /* --- --- --- property names --- --- --- */
    public static final String OP_DISTINCT_WINDOW = "fastersparql.op.distinct.window";
    public static final String OP_BIND_CONCURRENCY = "fastersparql.op.bind.concurrency";
    public static final String OP_JOIN_REORDER = "fastersparql.op.join.reorder";
    public static final String OP_JOIN_REORDER_BIND = "fastersparql.op.join.reorder.bind";
    public static final String OP_JOIN_REORDER_HASH = "fastersparql.op.join.reorder.hash";
    public static final String OP_JOIN_REORDER_WCO = "fastersparql.op.join.reorder.wco";
    public static final String OP_FILTER_PREFERRED_COMPILER = "fastersparql.op.filter.compiler";

    /* --- --- --- default values --- --- --- */
    public static final int DEF_OP_DISTINCT_WINDOW = 16384;
    public static final int DEF_OP_BIND_CONCURRENCY = 2;
    public static final String DEF_OP_JOIN_REORDER = "AvoidCartesian";
    public static final String DEF_OP_JOIN_REORDER_WCO = "Null";
    public static final String DEF_OP_FILTER_PREFERRED_COMPILER = null;


    /* --- --- --- accessors --- --- --- */

    /**
     * If a window-based implementation of {@link Distinct} is used, this property determines
     * the window size.
     *
     * A window size of {@code n} means the last {@code n} elements are kept in a set (e.g., a
     * {@link java.util.HashSet}) and incomming elements are compared for uniqueness within that
     * set. If the set overgrows {@code n}, the oldest elements are removed.
     *
     * @return a positive ({@code n > 0}) integer.
     */
    public static @Positive int distinctWindow() {
        return readPositiveInt(OP_DISTINCT_WINDOW, DEF_OP_DISTINCT_WINDOW);
    }

    /**
     * When executing a bind (left) join or bind minus, execute at most this number of
     * concurrent requests against the same source (considering only the current operator
     * execution: if this property has the value {@code n} and there are {@code m} executions
     * of a bind-implemented operator, then there may be up to {@code n*m} executions to a
     * single source if all operator executions target the same source).
     *
     * @return the maximum concurrency for bound operations in each execution of a bind-implemented
     *         operator.
     */
    public static @Positive int bindConcurrency() {
        return readPositiveInt(OP_BIND_CONCURRENCY, DEF_OP_BIND_CONCURRENCY);
    }


    private static final class JoinReorderStrategyParser implements Parser<JoinReorderStrategy> {
        private static final JoinReorderStrategyParser INSTANCE = new JoinReorderStrategyParser();
        @Override
        public JoinReorderStrategy parse(String src,
                                         String val) throws IllegalArgumentException {
            JoinReorderStrategy s = JoinHelpers.loadStrategy(val);
            if (s == null)
                throw new IllegalArgumentException("No JoinReorderStrategy found for "+src+"="+val);
            return s;
        }
    }

    /**
     * The {@link JoinReorderStrategy} to use for joins implemented with bind.
     *
     * The default strategy is {@link AvoidCartesianJoinReorderStrategy}, which only tries to
     * avoid cartesian products, retaining the original operand order as much as possible
     * (i.e., minimal optimization).
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
     * Same as {@link FasterSparqlOpProperties#bindJoinReorder()} but for hash-based joins.
     */
    public static JoinReorderStrategy hashJoinReorder() {
        JoinReorderStrategyParser p = JoinReorderStrategyParser.INSTANCE;
        JoinReorderStrategy s = readProperty(OP_JOIN_REORDER_HASH, null, p);
        if (s == null)
            s = readProperty(OP_JOIN_REORDER, AvoidCartesianJoinReorderStrategy.INSTANCE, p);
        return s;
    }

    /**
     * Same as {@link FasterSparqlOpProperties#bindJoinReorder()} but for worst-case optimal joins.
     */
    public static JoinReorderStrategy wcoJoinReorder() {
        JoinReorderStrategyParser p = JoinReorderStrategyParser.INSTANCE;
        JoinReorderStrategy s = readProperty(OP_JOIN_REORDER_WCO, null, p);
        if (s == null)
            s = readProperty(OP_JOIN_REORDER, NullJoinReorderStrategy.INSTANCE, p);
        return s;
    }

    /**
     * Name of the preferred {@link ExprEvaluatorCompilerProvider}.
     *
     * The default value is null, so that the provider with lowest
     * {@link ExprEvaluatorCompilerProvider#order()} will be selected.
     */
    public static String preferredExprCompiler() {
        return readTrimmedString(OP_FILTER_PREFERRED_COMPILER, null);
    }
}
