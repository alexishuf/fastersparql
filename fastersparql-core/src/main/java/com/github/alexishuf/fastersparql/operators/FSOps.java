package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.bind.BindExists;
import com.github.alexishuf.fastersparql.operators.bind.BindJoin;
import com.github.alexishuf.fastersparql.operators.bind.BindLeftJoin;
import com.github.alexishuf.fastersparql.operators.bind.BindMinus;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetricsListener;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.expr.Expr;
import com.github.alexishuf.fastersparql.sparql.expr.ExprParser;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressWarnings({"unused"})
public class FSOps {
    private static final Queue<PlanMetricsListener> listeners = new ConcurrentLinkedQueue<>();

    /**
     * Add a listener to receive metrics for any operator that gets executed after this call.
     * @param listener the listener
     */
    public static void addGlobalMetricsListener(PlanMetricsListener listener) {
        if (!listeners.contains(listener))
            listeners.add(listener);
    }

    /**
     * Removes a listener previously added via
     * {@link FSOps#addGlobalMetricsListener(PlanMetricsListener)}.
     *
     * @param listener the listener to remove
     * @return true iff the listener was previously added and previously removed (now it is removed).
     */
    public static boolean removeGlobalMetricsListener(PlanMetricsListener listener) {
        if (listener == null)
            return false;
        return listeners.remove(listener);
    }

    /**
     * Tests if there is at least one {@link PlanMetricsListener} added via
     * {@link FSOps#addGlobalMetricsListener(PlanMetricsListener)}.
     */
    public static boolean hasGlobalMetricsListeners() {
        return !listeners.isEmpty();
    }

    /**
     * Delivers a {@link PlanMetrics} instance to all listeners registered via
     * {@link FSOps#addGlobalMetricsListener(PlanMetricsListener)}.
     *
     * @param plan The {@link Plan} for which the {@code metrics} correspond
     * @param metrics the metrics instance.
     */
    public static void sendMetrics(Plan<?, ?> plan, PlanMetrics metrics) {
        for (PlanMetricsListener listener : listeners)
            listener.accept(plan, metrics);
    }

    /**
     * Get the {@link BindType} corresponding to the given plan
     * @return A {@link BindType} or {@code null} if plan is not a join/leftJoin/exists/minus.
     */
    public static @Nullable BindType bindTypeOf(Plan<?, ?> plan) {
        return switch (plan) {
            case Join<?, ?> join -> BindType.JOIN;
            case LeftJoin<?, ?> bindLeftJoin -> BindType.LEFT_JOIN;
            case Exists<?, ?> exists -> exists.negate() ? BindType.NOT_EXISTS : BindType.EXISTS;
            case Minus<?, ?> minus -> BindType.MINUS;
            case null, default -> null;
        };
    }

    public static <R, I> Empty<R, I> empty(RowType<R, I> rowType) {
        return new Empty<>(rowType, null, null, null, null);
    }

    public static <R, I> Query<R, I> query(SparqlClient<R, I, ?> client, CharSequence query) {
        return new Query<>(new SparqlQuery(query.toString()), client, null, null);
    }
    public static <R, I> Query<R, I> query(SparqlClient<R, I, ?> client, SparqlQuery query) {
        return new Query<>(query, client, null, null);
    }

    public static <R, I> Values<R, I> values(RowType<R, I> rowType, Vars vars, Iterable<R> rows) {
        List<R> list;
        if (rows instanceof List<R> l) {
            list = l;
        } else {
            list = new ArrayList<>(rows instanceof Collection<R> c ? c.size() : 10);
            for (R r : rows) list.add(r);
        }
        return new Values<>(rowType, vars, list, null, null);
    }

    public static <R, I> Join<R, I> join(List<? extends Plan<R, I>> operands) {
        if (operands.size() < 2)
            throw new IllegalArgumentException("Join requires 2+ operands, got "+operands.size());

        var ordered = FSOpsProperties.bindJoinReorder().reorder(operands, true);

        int last = ordered.size()-1;
        var root = ordered.get(0);
        for (int i = 1; i < last; i++)
            root = new BindJoin<>(root, ordered.get(i), null, null, null);

        Vars projection = null;
        if (ordered != operands) {
            projection = Vars.fromSet(operands.get(0).publicVars());
            for (int i = 1, n = operands.size(); i < n; i++)
                projection = projection.union(operands.get(i).publicVars());
        }
        return new BindJoin<>(root, ordered.get(last), projection, null, null);
    }

    public static <R, I> Join<R, I> join(Plan<R, I> left, Plan<R, I> right) {
        return join(List.of(left, right));
    }


    public static <R, I> Union<R, I> union(List<? extends Plan<R, I>> operands) {
        return new Union<>(operands, 0, null, null);
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
     * @param crossDedupCapacity at most {@code operands.size()crossDedupCapacity} rows will
     *                           be kept in main memory to perform the de-duplication.
     */
    public static <R, I> Union<R, I> union(List<? extends Plan<R, I>> operands,
                                           int crossDedupCapacity) {
        return new Union<>(operands, crossDedupCapacity, null, null);
    }

    /** Equivalent to {@link FSOps#union(List, int)} with {@link FSOpsProperties#dedupCapacity()} */
    public static <R, I> Union<R, I> crossDedupUnion(List<? extends Plan<R, I>> operands) {
        return union(operands, FSOpsProperties.dedupCapacity());
    }

    public static <R, I> LeftJoin<R, I> leftJoin(Plan<R, I> left, Plan<R, I> right) {
        return new BindLeftJoin<>(left, right, null, null);
    }

    /**
     * Apply the given modifiers around {@code input}. If {@code input} already is a
     * {@link Modifier}, apply to its first (and only) operand.
     *
     * <p>If all modifiers are given simultaneously, the SPARQL algebra that is executed is
     * Limit(limit, Offset(offset, Distinct(Project[projection](Filter(input, filters...)))))</p>
     *
     * @param input input of the modifier. May also be a {@link Modifier}.
     * @param projection If non-null applies a projection of the given vars over {@code input}
     * @param distinctWindow If 0, do not apply DISTINCT; If {@link Integer#MAX_VALUE}, apply
     *                       strict DISTINCT semantics; else apply weaker DISTINCT semantics
     *                       but keep at most {@code distinctWindow} rows in main memory, which
     *                       might lead to duplicate rows not being filtered-out.
     * @param offset discard the first {@code offset} rows that passed Filter and Distinct evaluation
     * @param limit discard all subsequent rows after {@code offset+limit} rows passed Filter
     *              and Distinct evaluation
     * @param filters set of filter expressions that if evaluate to false or fail to execute cause
     *                rows from {@code input} to be discarded before evaluation of other modifiers.
     *                These may be {@link Expr} instances or any {@link Object} whose
     *                {@link Object#toString()} evaluates to a valid SPARQL expression.
     */
    public static <R, I> Modifier<R, I> modifiers(Plan<R, I> input, @Nullable Vars projection,
                                                  int distinctWindow, long offset, long limit,
                                                  Collection<?> filters) {
        List<Expr> parsed = parseFilters(input, filters);
        if (input instanceof Modifier<R,I>)
            input = input.operands.get(0);
        return new Modifier<>(input, projection, distinctWindow, offset, limit,
                              parsed, null, null);
    }

    /**
     * Once {@code input} produces {@code limit} rows, close it and discard any subsequent rows.
     *
     * @param input the source of rows to discard. If a {@link Modifier}, all other modifiers
     *              will be kept.
     * @param limit maximum number of rows to output.
     */
    public static <R, I> Modifier<R, I> limit(Plan<R, I> input, long limit) {
        if (input instanceof Modifier<R,I> m) {
            return new Modifier<>(m.operands.get(0), m.projection, m.distinctCapacity, m.offset,
                                  limit, m.filters, m.unbound, m.name);
        }
        return new Modifier<>(input, null, 0, 0, limit,
                             null, null, null);
    }

    /**
     * Discard the first {@code offset} rows of {@code input}
     * @param input the source of rows to discard. If a {@link Modifier}, all other modifiers
     *              will be kept.
     * @param offset how many rows to skip.
     */
    public static <R, I> Modifier<R, I> offset(Plan<R, I> input, long offset) {
        if (input instanceof Modifier<R,I> m) {
            return new Modifier<>(m.operands.get(0), m.projection, m.distinctCapacity, offset,
                                  m.limit, m.filters, m.unbound, m.name);
        }
        return new Modifier<>(input, null, 0, offset, Long.MAX_VALUE,
                             null, null, null);
    }

    /** Equivalent to {@link FSOps#limit(Plan, long)} over {@link FSOps#offset(Plan, long)}. */
    public static <R, I> Modifier<R, I> slice(Plan<R, I> input, long offset, long limit) {
        if (input instanceof Modifier<R,I> m) {
            return new Modifier<>(m.operands.get(0), m.projection, m.distinctCapacity, offset,
                                  limit, m.filters, m.unbound, m.name);
        }
        return new Modifier<>(input, null, 0, offset, limit,
                             null, null, null);
    }

    /** Equivalent to {@link FSOps#distinct(Plan, int)} with {@link Integer#MAX_VALUE}. */
    public static <R, I> Modifier<R, I> distinct(Plan<R, I> input) {
        return distinct(input, Integer.MAX_VALUE);
    }

    /** Equivalent to {@link FSOps#distinct(Plan, int)} with {@link FSOpsProperties#distinctCapacity()}. */
    public static <R, I> Modifier<R, I> boundedDistinct(Plan<R, I> input) {
        return distinct(input, FSOpsProperties.distinctCapacity());
    }

    /** Equivalent to {@link FSOps#distinct(Plan, int)} with {@link FSOpsProperties#reducedCapacity()}. */
    public static <R, I> Modifier<R, I> reduced(Plan<R, I> input) {
        return distinct(input, FSOpsProperties.reducedCapacity());
    }

    /** Equivalent to {@link FSOps#distinct(Plan, int)} with {@link FSOpsProperties#dedupCapacity()}. */
    public static <R, I> Modifier<R, I> dedup(Plan<R, I> input) {
        return distinct(input, FSOpsProperties.dedupCapacity());
    }

    /**
     * Deduplicate the rows from {@code input} keeping a history of at most {@code capacity}
     * previous rows from {@code input}.
     *
     * @param input source of rows to deduplicate
     * @param capacity maximum number of previous rows to be kept in memory for performing
     *                 de-duplication. {@link Integer#MAX_VALUE} will implement a proper
     *                 DISTINCT. See also {@link FSOpsProperties#distinctCapacity()},
     *                 {@link FSOpsProperties#reducedCapacity()} and
     *                 {@link FSOpsProperties#dedupCapacity()}.
     */
    public static <R, I> Modifier<R, I> distinct(Plan<R, I> input, int capacity) {
        if (input instanceof Modifier<R,I> m) {
            return new Modifier<>(m.operands.get(0), m.projection, capacity, m.offset,
                                  m.limit, m.filters, m.unbound, m.name);
        }
        return new Modifier<>(input, null, capacity, 0, Long.MAX_VALUE, null, null, null);
    }

    /**
     * Apply a projection to the rows of {@code input}, so that {@code publicVars().equals(vars)}.
     *
     * @param input source of rows to project. If it is a {@link Modifier} all other modifiers
     *             (except projection) are kept.
     * @param vars the projection.
     */
    public static <R, I> Modifier<R, I> project(Plan<R, I> input, Vars vars) {
        if (input instanceof Modifier<R,I> m) {
            return new Modifier<>(m.operands.get(0), vars, m.distinctCapacity, m.offset,
                                  m.limit, m.filters, m.unbound, m.name);
        }
        return new Modifier<>(input, vars, 0, 0, Long.MAX_VALUE,
                              null, null, null);
    }

    private static List<Expr> parseFilters(Plan<?,?> maybeModifier, Collection<?> filters) {
        List<Expr> oldFilters = maybeModifier instanceof Modifier<?,?> m ? m.filters : List.of();
        int size = oldFilters.size() + filters.size();
        List<Expr> parsed = size == 0 ? List.of() : new ArrayList<>(size);
        //noinspection ConstantConditions
        parsed.addAll(oldFilters);
        var p = new ExprParser();
        for (Object o : filters) {
            if (o instanceof Expr e)
                parsed.add(e);
            else if (o != null)
                parsed.add(p.parse(o.toString()));
        }
        return parsed;
    }

    /** See {@link FSOps#filter(Plan, Collection)} */
    public static <R, I> Modifier<R, I> filter(Plan<R, I> input, String... filters) { return filter(input, Arrays.asList(filters)); }


    /**
     * Discard rows from input for which at least one filter evaluates to false or fails to execute.
     *
     * @param input input of the modifier. If this is a {@link Modifier}, its filters will be
     *              prepended to {@code filters} and all other modifiers will be kept.
     * @param filters list of {@link Expr} or Strings with valid SPARQL expressions.
     */
    public static <R, I> Modifier<R, I> filter(Plan<R, I> input, Collection<?> filters) {
        var parsed = parseFilters(input, filters);
        if (input instanceof Modifier<R,I> m) {
            return new Modifier<>(input.operands.get(0), m.projection, m.distinctCapacity,
                                  m.offset, m.limit, parsed, m.unbound, input.name);
        } else {
            return new Modifier<>(input, null, 0, 0, Long.MAX_VALUE,
                                  parsed, null, null);
        }
    }

    public static <R, I> Exists<R, I> exists(Plan<R, I> input, boolean negate, Plan<R, I> filter) {
        return new BindExists<>(input, negate, filter, null, null);
    }

    public static <R, I> Minus<R, I> minus(Plan<R, I> left, Plan<R, I> right) {
        return new BindMinus<>(left, right, null, null);
    }
}
