package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetricsListener;
import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.operators.providers.OperatorProvider;
import com.github.alexishuf.fastersparql.operators.providers.OperatorProviderRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressWarnings("unchecked")
public class FasterSparqlOps {
    private static final OperatorProviderRegistry registry
            = new OperatorProviderRegistry().registerAll();
    private static final Queue<PlanMetricsListener> listeners = new ConcurrentLinkedQueue<>();

    /**
     * Add a listener to receive metrics for any operator that gets executed after this call.
     * @param listener the listener
     */
    public static void addGlobalMetricsListener(@lombok.NonNull PlanMetricsListener listener) {
        if (!listeners.contains(listener))
            listeners.add(listener);
    }

    /**
     * Removes a listenerr previously added via
     * {@link FasterSparqlOps#addGlobalMetricsListener(PlanMetricsListener)}.
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
     * {@link FasterSparqlOps#addGlobalMetricsListener(PlanMetricsListener)}.
     */
    public static boolean hasGlobalMetricsListeners() {
        return !listeners.isEmpty();
    }

    /**
     * Delivers a {@link PlanMetrics} instance to all listeners registered via
     * {@link FasterSparqlOps#addGlobalMetricsListener(PlanMetricsListener)}.
     *
     * @param plan The {@link Plan} for which the {@code metrics} correspond
     * @param metrics the metrics instance.
     */
    public static <R> void sendMetrics(Plan<R> plan, PlanMetrics metrics) {
        for (PlanMetricsListener listener : listeners)
            listener.accept(plan, metrics);
    }

    /**
     * Create an instance of the given {@link Operator} interface that best matches the given flags.
     *
     * Creation of an {@link Operator} instance is done by the {@link OperatorProvider}
     * corresponding to the {@link Operator} sub-interface given by {@code cls}. If there is more
     * than one suitable provider the one that produces the lowest
     * {@link OperatorProvider#bid(long)} for the given {@code flags} will be selected.
     *
     * @param cls An interface extending {@link Operator} denoting the {@link Operator} type to
     *            be instantiated.
     * @param flags A bitset of flags which provide information about the operands to be passed,
     *             about desired characteristics of the {@link Operator} implementation and
     *              about permissions for the implementation to deviate from standard SPARQL
     *              semantics. See {@link OperatorFlags} for a list of built-in flags.
     * @throws NoOperatorProviderException if there is no {@link OperatorProvider} for the given
     *         {@code cls} that returns a {@link OperatorProvider#bid(long)} for {@code flags}
     *         below {@link Integer#MAX_VALUE}.
     */
    public static <T extends Operator> T
    create(Class<T> cls, long flags, Class<?> rowClass) throws NoOperatorProviderException {
        RowOperations ops = RowOperationsRegistry.get().forClass(rowClass);
        //noinspection unchecked
        return (T)registry.get(OperatorName.valueOf(cls), flags).create(flags, ops);
    }

    /**
     * Equivalent to {@link FasterSparqlOps#create(Class, long, Class)} with {@code name.asClass()}.
     */
    public static Operator create(OperatorName name, long flags,
                                  Class<?> rowClass) throws NoOperatorProviderException {
        RowOperations ops = RowOperationsRegistry.get().forClass(rowClass);
        return registry.get(name, flags).create(flags, ops);
    }

    public static <R> EmptyPlan.EmptyPlanBuilder<R> empty(Class<? super R> rowClass) {
        return EmptyPlan.<R>builder().rowClass(rowClass);
    }

    public static <R> JoinPlan.JoinPlanBuilder<R> join(List<? extends Plan<R>> inputs,
                                                       Class<? super R> rowClass, long flags) {
        return create(Join.class, flags, rowClass).<R>asPlan().operands(inputs);
    }
    public static <R> JoinPlan.JoinPlanBuilder<R> join(List<? extends Plan<R>> inputs,
                                                       Class<? super R> rowClass) {
        return create(Join.class, OperatorFlags.ASYNC, rowClass).<R>asPlan().operands(inputs);
    }

    public static <R> UnionPlan.UnionPlanBuilder<R> union(List<? extends Plan<R>> inputs,
                                                          Class<? super R> rowClass, long flags) {
        return create(Union.class, flags, rowClass).<R>asPlan().inputs(inputs);
    }
    public static <R> UnionPlan.UnionPlanBuilder<R> union(List<? extends Plan<R>> inputs,
                                                          Class<? super R> rowClass) {
        return create(Union.class, OperatorFlags.ASYNC, rowClass).<R>asPlan().inputs(inputs);
    }

    public static <R> LeftJoinPlan.LeftJoinPlanBuilder<R>
    leftJoin(Plan<R> left, Plan<R> right, Class<? super R> rowClass, long flags) {
        return create(LeftJoin.class, flags, rowClass).<R>asPlan().left(left).right(right);
    }
    public static <R> LeftJoinPlan.LeftJoinPlanBuilder<R>
    leftJoin(Plan<R> left, Plan<R> right, Class<? super R> rowClass) {
        return create(LeftJoin.class, 0L, rowClass).<R>asPlan().left(left).right(right);
    }

    public static <R> SlicePlan.SlicePlanBuilder<R> slice(Plan<R> input,
                                         Class<? super R> rowClass, long flags) {
        return create(Slice.class, flags, rowClass).<R>asPlan().input(input);

    }
    public static <R> SlicePlan.SlicePlanBuilder<R> slice(Plan<R> input,
                                         Class<? super R> rowClass) {
        return create(Slice.class, 0L, rowClass).<R>asPlan().input(input);
    }

    public static <R> DistinctPlan.DistinctPlanBuilder<R>
    distinct(Plan<R> input, Class<? super R> rowClass, long flags) {
        return create(Distinct.class, flags, rowClass).<R>asPlan().input(input);
    }
    public static <R> DistinctPlan.DistinctPlanBuilder<R>
    distinct(Plan<R> input, Class<? super R> rowClass) {
        return create(Distinct.class, 0L, rowClass).<R>asPlan().input(input);
    }

    public static <R> ProjectPlan.ProjectPlanBuilder<R>
    project(Plan<R> input, List<String> vars, Class<? super R> rowClass, long flags) {
        return create(Project.class, flags, rowClass).<R>asPlan().input(input).vars(vars);
    }
    public static <R> ProjectPlan.ProjectPlanBuilder<R>
    project(Plan<R> input, List<String> vars, Class<? super R> rowClass) {
        return create(Project.class, 0L, rowClass).<R>asPlan().input(input).vars(vars);
    }

    public static <R> FilterPlan.FilterPlanBuilder<R>
    filter(Plan<R> input, Collection<? extends CharSequence> filters,
           Class<? super R> rowClass, long flags) {
        List<String> filtersList;
        boolean ok = false;
        if (filters instanceof List) {
            ok = true;
            for (CharSequence filter : filters) {
                if (!(filter instanceof String)) {
                    ok = false;
                    break;
                }
            }
        }
        if (ok) {
            filtersList = (List<String>) filters;
        } else {
            filtersList = new ArrayList<>(filters.size());
            for (CharSequence filter : filters) filtersList.add(filter.toString());
        }
        return create(Filter.class, flags, rowClass).<R>asPlan().input(input).filters(filtersList);
    }
    public static <R> FilterPlan.FilterPlanBuilder<R>
    filter(Plan<R> input, Collection<? extends CharSequence> filters, Class<? super R> rowClass) {
        return filter(input, filters, rowClass, 0L);
    }

    public static <R> FilterExistsPlan.FilterExistsPlanBuilder<R>
    filterExists(Plan<R> input, boolean negate, Plan<R> filter,
                 Class<? super R> rowClass, long flags) {
        return create(FilterExists.class, flags, rowClass).<R>asPlan()
                .input(input).negate(negate).filter(filter);
    }
    public static <R> FilterExistsPlan.FilterExistsPlanBuilder<R>
    filterExists(Plan<R> input, boolean negate, Plan<R> filter, Class<? super R> rowClass) {
        return filterExists(input, negate, filter, rowClass, 0L);
    }

    public static <R> MinusPlan.MinusPlanBuilder<R>
    minus(Plan<R> left, Plan<R> right, Class<? super R> rowClass, long flags) {
        return create(Minus.class, flags, rowClass).<R>asPlan().left(left).right(right);
    }
    public static <R> MinusPlan.MinusPlanBuilder<R>
    minus(Plan<R> left, Plan<R> right, Class<? super R> rowClass) {
        return create(Minus.class, 0L, rowClass).<R>asPlan().left(left).right(right);
    }
}
