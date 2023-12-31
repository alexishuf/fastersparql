package com.github.alexishuf.fastersparql;

import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClientFactory;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.OpaqueSparqlQuery;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprParser;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.github.alexishuf.fastersparql.client.UnboundSparqlClient.UNBOUND_CLIENT;
import static com.github.alexishuf.fastersparql.client.model.SparqlConfiguration.EMPTY;

@SuppressWarnings("unused")
public class FS {
    private static final Logger log = LoggerFactory.getLogger(FS.class);

    /* --- --- --- client factory methods --- --- --- */

    private static List<SparqlClientFactory> FACTORIES = null;

    /** Re-scans the classpath for {@link SparqlClientFactory} implementations. */
    public static List<SparqlClientFactory> reloadFactories() {
        ArrayList<SparqlClientFactory> list = new ArrayList<>();
        for (var f : ServiceLoader.load(SparqlClientFactory.class)) list.add(f);
        return FACTORIES = list;
    }

    /**
     * Creates a {@link SparqlClient} with given {@code rowType} for querying {@code endpoint}.
     *
     * @param endpoint the {@link SparqlEndpoint} to be queried
     * @param preferredTag If non-null, {@link SparqlClientFactory} implementations that have this
     *                     {@link SparqlClientFactory#tag()} will be preferred over other factories
     *                     that have lower {@link SparqlClientFactory#order()} but different tag.
     * @return a {@link SparqlClient} bound to the given {@link SparqlClient#endpoint()}
     */
    public static SparqlClient
    clientFor(SparqlEndpoint endpoint, String preferredTag) {
        SparqlClientFactory tagged = null, untagged = null;
        int taggedOrder = Integer.MAX_VALUE, untaggedOrder = Integer.MAX_VALUE;
        for (var fac : (FACTORIES == null ? reloadFactories() : FACTORIES)) {
            if (!fac.supports(endpoint)) continue;
            if (preferredTag != null && fac.tag().equalsIgnoreCase(preferredTag)) {
                if (taggedOrder > fac.order())
                    tagged = fac;
            } else if (untaggedOrder > fac.order()) {
                untagged = fac;
            }
        }
        var best = tagged != null ? tagged : untagged;
        if (best == null) {
            var msg = "No SparqlClientFactory supports the given endpoint";
            throw new UnacceptableSparqlConfiguration(endpoint.uri(), EMPTY,
                                                      endpoint.configuration(), msg);
        }
        return best.createFor(endpoint);
    }

    public static SparqlClient clientFor(SparqlEndpoint endpoint) {
        return clientFor(endpoint, null);
    }

    /* --- --- --- shutdown hooks --- --- --- */

    private static final Queue<Runnable> shutdownHooks = new ConcurrentLinkedQueue<>();

    /**
     * Register a Runnable to execute when {@link FS#shutdown()} is called.
     *
     * <p>Once {@link FS#shutdown()} is called, the hook will be de-registered after
     * execution. If resources needing a shutdown hook are re-acquired after a
     * {@link FS#shutdown()}, this method must be called again to ensure the hook
     * will execute on a second {@link FS#shutdown()}  call.</p>
     *
     * @param runnable code to execute on {@link FS#shutdown()}.
     */
    public static void addShutdownHook(Runnable runnable) { shutdownHooks.add(runnable); }

    /**
     * Releases globally held resources initialized by fastersparql components.
     *
     * <p>One example of such resources are Netty {@code EventLoopGroup} and the associated threads.
     * If this method is not called, it may take a few seconds for the {@code EventLoopGroup} to
     * be closed by its keep-alive timeout.</p>
     */
    public static void shutdown() {
        for (Runnable hook = shutdownHooks.poll(); hook != null; hook = shutdownHooks.poll()) {
            try {
                hook.run();
            } catch (Throwable t) {
                System.err.println("Failed to execute shutdown hook "+hook+": "+t);
            }
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(FS::shutdown, "FS::shutdown"));
    }

    /* --- --- --- factory methods --- --- --- */

    public static Empty empty() { return Empty.EMPTY; }

    public static Query query(SparqlClient client, CharSequence query) {
        return new Query(new OpaqueSparqlQuery(new ByteRope(query)), client);
    }

    public static Query query(SparqlClient client, Rope query) {
        return new Query(new OpaqueSparqlQuery(query), client);
    }

    public static Query query(SparqlClient client, SparqlQuery query) {
        return new Query(query, client);
    }

    public static Query query(CharSequence query) {
        return new Query(new OpaqueSparqlQuery(new ByteRope(query)), UNBOUND_CLIENT);
    }

    public static Query query(Rope query) {
        return new Query(new OpaqueSparqlQuery(query), UNBOUND_CLIENT);
    }

    public static Query query(SparqlQuery query) {
        return new Query(query, UNBOUND_CLIENT);
    }

    public static Values values(Vars vars, Collection<?> rows) {
        TermBatch b;
        int cols = vars.size(), rowsSize = rows.size();
        if (rowsSize == 0) {
            b = null;
        } else {
            b = new TermBatch(new Term[rowsSize*cols], 0, cols, true);
            b.markUntracked();
            for (Object row : rows) {
                switch (row) {
                    case Term[] a -> b.putRow(a);
                    case Collection<?> c -> b.putRow(c);
                    case null -> throw new IllegalArgumentException("Unexpected null in rows");
                    default -> throw new UnsupportedOperationException("Unexpected row type");
                }
            }
        }
        return new Values(vars, b);
    }

    public static Plan join(Plan left, Plan right) {
        if (left.type == Operator.JOIN || right.type == Operator.JOIN)
            return makeNaryJoin(flattenJoin(left, right));
        var p = FSProperties.bindJoinReorder().reorder(left, right);
        return p == null ? new Join(null, left, right) : new Join(p, right, left);
    }

    private static Plan makeNaryJoin(Plan... operands) {
        if (operands.length < 2)
            return operands.length == 0 ? Empty.EMPTY : operands[0];
        Vars projection = FSProperties.bindJoinReorder().reorder(operands);
        return new Join(projection, operands);
    }

    public static Plan join(Plan... operands) { return makeNaryJoin(flattenJoin(operands)); }

    static Plan[] flattenJoin(Plan... ops) {
        int flatSize = -1;
        for (Plan op : ops) {
            if (op instanceof Join j) flatSize = Math.max(0, flatSize) + j.opCount()-1;
        }
        if (flatSize == -1)
            return ops;

        Plan[] flat = new Plan[ops.length+flatSize];
        flatSize = 0;
        for (Plan o : ops) {
            if (o instanceof Join) {
                for (int i = 0, n = o.opCount(); i < n; i++)
                    flat[flatSize++] = o.op(i);
            } else {
                flat[flatSize++] = o;
            }
        }
        return flat;
    }

    static Plan[] flattenUnion(boolean crossDedup, Plan... ops) {
        int flatSize = -1;
        for (Plan o : ops) {
            if (o instanceof Union u && u.crossDedup == crossDedup)
                flatSize = Math.max(0, flatSize) + o.opCount()-1;
        }
        if (flatSize == -1) return ops;

        Plan[] flat = new Plan[ops.length+flatSize];
        flatSize = 0;
        for (Plan o : ops) {
            if (o instanceof Union u && u.crossDedup == crossDedup) {
                for (int i = 0, n = o.opCount(); i < n; i++)
                    flat[flatSize++] = o.op(i);
            } else {
                flat[flatSize++] = o;
            }
        }
        return flat;
    }

    public static Union union(Plan... operands) {
        return new Union(false, flattenUnion(false, operands));
    }

    /**
     * If {@code crossDedupCapacity > 0}, computes a union of the operands but try to discard
     * any row A that has been previously output by any other operand that not the one that
     * just produced that row.
     *
     * <p>The use case is federated queries where data duplicated across sources may
     * generate bogus duplicates.</p>
     *
     * @param operands the union operands
     * @param crossDedup at most {@code operands.size()crossDedupCapacity} rows will
     *                           be kept in main memory to perform the de-duplication.
     */
    public static Union union(boolean crossDedup, Plan... operands) {
        return new Union(crossDedup, flattenUnion(crossDedup, operands));
    }

    /** Equivalent to {@link FS#union(boolean, Plan...)} with {@link FSProperties#crossDedup()} */
    public static Union crossDedupUnion(Plan... operands) {
        boolean crossDedup = FSProperties.crossDedup();
        return new Union(crossDedup, flattenUnion(crossDedup, operands));
    }

    public static LeftJoin leftJoin(Plan left, Plan right) {
        return new LeftJoin(left, right);
    }

    /**
     * Apply the given modifiers around {@code input}. If {@code input} already is a
     * {@link Modifier}, apply to its first (and only) operand.
     *
     * <p>If {@code input} is a modifier, no-op values for parameters {@code projection},
     * {@code distinct}, {@code offset}, {@code limit} and {@code filters} will be
     * replaced with the values set in {@code input}. In the specific case of {@code filters},
     * the expressions given in this call will be concatenated with any filters in {@code input}.</p>
     *
     * <p>If all modifiers are given simultaneously, the SPARQL algebra that is executed is
     * Limit(limit, Offset(offset, Distinct(Project[projection](Filter(input, filters...)))))</p>
     *
     * @param input input of the modifier. May also be a {@link Modifier}.
     * @param projection If non-null applies a projection of the given vars over {@code input}
     * @param distinct If 0, do not apply DISTINCT; If {@link Integer#MAX_VALUE}, apply
     *                       strict DISTINCT semantics; else apply weaker DISTINCT semantics
     *                       but keep at most {@code distinct} rows in main memory, which
     *                       might lead to duplicate rows not being filtered-out.
     * @param offset discard the first {@code offset} rows that passed Filter and Distinct evaluation
     * @param limit discard all subsequent rows after {@code offset+limit} rows passed Filter
     *              and Distinct evaluation
     * @param filters set of filter expressions that if evaluate to false or fail to execute cause
     *                rows from {@code input} to be discarded before evaluation of other modifiers.
     *                These may be {@link Expr} instances or any {@link Object} whose
     *                {@link Object#toString()} evaluates to a valid SPARQL expression.
     */
    public static Plan modifiers(Plan input, @Nullable Vars projection,
                                              DistinctType distinct, long offset, long limit,
                                              Collection<?> filters) {
        boolean nop = distinct == null
                && offset == 0 && limit == Long.MAX_VALUE
                && filters.isEmpty()
                && (projection == null || projection.equals(input.publicVars()));
        if (nop)
            return input;
        List<Expr> parsed = parseFilters(input, filters);
        if (input instanceof Modifier m) {
            input = input.left;
            if (projection == null)
                projection = m.projection;
            if (distinct == null)
                distinct = m.distinct;
            if (offset == 0)
                offset = m.offset;
            if (limit == Long.MAX_VALUE)
                limit = m.limit;
        }
        return new Modifier(input, projection, distinct, offset, limit,
                            parsed);
    }

    /**
     * Once {@code input} produces {@code limit} rows, close it and discard any subsequent rows.
     *
     * @param input the source of rows to discard. If a {@link Modifier}, all other modifiers
     *              will be kept.
     * @param limit maximum number of rows to output.
     */
    public static Modifier limit(Plan input, long limit) {
        if (input instanceof Modifier m) {
            return new Modifier(m.left, m.projection, m.distinct, m.offset,
                                  limit, m.filters);
        }
        return new Modifier(input, null, null, 0, limit, null);
    }

    /**
     * Discard the first {@code offset} rows of {@code input}
     * @param input the source of rows to discard. If a {@link Modifier}, all other modifiers
     *              will be kept.
     * @param offset how many rows to skip.
     */
    public static Modifier offset(Plan input, long offset) {
        if (input instanceof Modifier m) {
            return new Modifier(m.left, m.projection, m.distinct, offset,
                                  m.limit, m.filters);
        }
        return new Modifier(input, null, null, offset, Long.MAX_VALUE, null);
    }

    /** Equivalent to {@link FS#limit(Plan, long)} over {@link FS#offset(Plan, long)}. */
    public static Modifier slice(Plan input, long offset, long limit) {
        if (input instanceof Modifier m) {
            return new Modifier(m.left, m.projection, m.distinct, offset,
                                  limit, m.filters);
        }
        return new Modifier(input, null, null, offset, limit, null);
    }

    /** Equivalent to {@link FS#distinct(Plan, DistinctType)} with {@link Integer#MAX_VALUE}. */
    public static Modifier distinct(Plan input) {
        return distinct(input, DistinctType.STRONG);
    }

    /** Equivalent to {@link FS#distinct(Plan, DistinctType)} with {@link DistinctType#STRONG}. */
    public static Modifier boundedDistinct(Plan input) {
        return distinct(input, DistinctType.STRONG);
    }

    /** Equivalent to {@link FS#distinct(Plan, DistinctType)} with {@link DistinctType#REDUCED}. */
    public static Modifier reduced(Plan input) {
        return distinct(input, DistinctType.REDUCED);
    }

    /** Equivalent to {@link FS#distinct(Plan, DistinctType)} with {@link DistinctType#WEAK}
     * if {@link FSProperties#opportunisticDedup()}. */
    public static Modifier dedup(Plan input) {
        return distinct(input,  FSProperties.opportunisticDedup() ? DistinctType.WEAK : null);
    }

    /**
     * Deduplicate the rows from {@code input} keeping a history of at most {@code type}
     * previous rows from {@code input}.
     *
     * @param input source of rows to deduplicate
     * @param type type de-duplication.
     */
    public static Modifier distinct(Plan input, DistinctType type) {
        if (input instanceof Modifier m) {
            return new Modifier(m.left, m.projection, type, m.offset, m.limit, m.filters);
        }
        return new Modifier(input, null, type, 0, Long.MAX_VALUE, null);
    }

    /**
     * Apply a projection to the rows of {@code input}, so that {@code publicVars().equals(vars)}.
     *
     * @param input source of rows to project. If it is a {@link Modifier} all other modifiers
     *             (except projection) are kept.
     * @param vars the projection.
     */
    public static Plan project(Plan input, Vars vars) {
        if (vars.equals(input.publicVars()))
            return input;
        if (input instanceof Modifier m) {
            return new Modifier(m.left, vars, m.distinct, m.offset, m.limit, m.filters);
        }
        return new Modifier(input, vars, null, 0, Long.MAX_VALUE, null);
    }

    private static List<Expr> parseFilters(Plan maybeModifier, Collection<?> filters) {
        List<Expr> oldFilters = maybeModifier instanceof Modifier m ? m.filters : List.of();
        int size = oldFilters.size() + filters.size();
        if (size == 0)
            return List.of();
        List<Expr> parsed = new ArrayList<>(size);
        parsed.addAll(oldFilters);
        var p = new ExprParser();
        for (Object o : filters) {
            if (o instanceof Expr e)
                parsed.add(e);
            else if (o != null)
                parsed.add(p.parse(SegmentRope.of(o)));
        }
        return parsed;
    }

    /** See {@link FS#filter(Plan, Collection)} */
    public static Modifier filter(Plan input, String... filters) { return filter(input, Arrays.asList(filters)); }

    /**
     * Discard rows from input for which at least one filter evaluates to false or fails to execute.
     *
     * @param input input of the modifier. If this is a {@link Modifier}, its filters will be
     *              prepended to {@code filters} and all other modifiers will be kept.
     * @param filters list of {@link Expr} or Strings with valid SPARQL expressions.
     */
    public static Modifier filter(Plan input, Collection<?> filters) {
        var parsed = parseFilters(input, filters);
        if (input instanceof Modifier m) {
            return new Modifier(input.left, m.projection, m.distinct,
                                m.offset, m.limit, parsed);
        } else {
            return new Modifier(input, null, null, 0, Long.MAX_VALUE,
                                parsed);
        }
    }

    public static Modifier filter(Plan input, List<Expr> filters) {
        if (input instanceof Modifier m) {
            List<Expr> union;
            if (m.filters.isEmpty()) {
                union = filters;
            } else {
                union = new ArrayList<>(m.filters.size()+filters.size());
                union.addAll(m.filters);
                union.addAll(filters);
            }
            return new Modifier(input.left, m.projection, m.distinct,
                                m.offset, m.limit, union);
        } else {
            return new Modifier(input, null, null, 0, Long.MAX_VALUE,
                                filters);
        }
    }

    public static Exists    exists(Plan in, boolean negate, Plan filter) { return new Exists(in, negate, filter); }
    public static Exists    exists(Plan in,                 Plan filter) { return new Exists(in, false, filter); }
    public static Exists notExists(Plan in,                 Plan filter) { return new Exists(in, true, filter); }
    public static Minus      minus(Plan in,                 Plan filter) { return new Minus(in, filter); }
}
