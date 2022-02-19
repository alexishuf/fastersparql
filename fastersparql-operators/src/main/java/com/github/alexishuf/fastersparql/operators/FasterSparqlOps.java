package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.operators.plan.*;
import com.github.alexishuf.fastersparql.operators.providers.OperatorProvider;
import com.github.alexishuf.fastersparql.operators.providers.OperatorProviderRegistry;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FasterSparqlOps {
    private static final OperatorProviderRegistry registry
            = new OperatorProviderRegistry().registerAll();

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

    public static <R> EmptyPlan<R> empty(List<String> publicVars, List<String> allVars,
                                         Class<? super R> rowClass) {
        return new EmptyPlan<>(publicVars, allVars, rowClass);
    }
    public static <R> EmptyPlan<R> empty(List<String> publicVars,
                                         Class<? super R> rowClass) {
        return new EmptyPlan<>(publicVars, publicVars, rowClass);
    }
    public static <R> EmptyPlan<R> empty(Class<? super R> rowClass) {
        return new EmptyPlan<>(Collections.emptyList(), Collections.emptyList(), rowClass);
    }

    public static <R> JoinPlan<R> join(List<Plan<R>> operands, Class<? super R> rowClass, long flags) {
        return new JoinPlan<>(create(Join.class, flags, rowClass), operands);
    }
    public static <R> JoinPlan<R> join(List<Plan<R>> operands, Class<? super R> rowClass) {
        return join(operands, rowClass, OperatorFlags.ASYNC);
    }

    public static <R> UnionPlan<R> union(List<Plan<R>> operands, Class<? super R> rowClass, long flags) {
        return new UnionPlan<>(create(Union.class, flags, rowClass), operands);
    }
    public static <R> UnionPlan<R> union(List<Plan<R>> operands, Class<? super R> rowClass) {
        return union(operands, rowClass, OperatorFlags.ASYNC);
    }

    public static <R> LeftJoinPlan<R> leftJoin(Plan<R> left, Plan<R> right, Class<? super R> rowClass,
                                               long flags) {
        return new LeftJoinPlan<>(create(LeftJoin.class, flags, rowClass), left, right);
    }
    public static <R> LeftJoinPlan<R> leftJoin(Plan<R> left, Plan<R> right, Class<? super R> rowClass) {
        return leftJoin(left, right, rowClass, 0L);
    }

    public static <R> SlicePlan<R> slice(Plan<R> input, long offset, long limit,
                                         Class<? super R> rowClass, long flags) {
        return new SlicePlan<>(create(Slice.class, flags, rowClass), input, offset, limit);
    }
    public static <R> SlicePlan<R> slice(Plan<R> input, long offset, long limit,
                                         Class<? super R> rowClass) {
        return slice(input, offset, limit, rowClass, 0L);
    }

    public static <R> DistinctPlan<R> distinct(Plan<R> input, Class<? super R> rowClass,
                                               long flags) {
        return new DistinctPlan<>(create(Distinct.class, flags, rowClass), input);
    }
    public static <R> DistinctPlan<R> distinct(Plan<R> input, Class<? super R> rowClass) {
        return distinct(input, rowClass, 0L);
    }

    public static <R> ProjectPlan<R> project(Plan<R> input, List<String> vars,
                                             Class<? super R> rowClass, long flags) {
        return new ProjectPlan<>(create(Project.class, flags, rowClass), input, vars);
    }
    public static <R> ProjectPlan<R> project(Plan<R> input, List<String> vars,
                                             Class<? super R> rowClass) {
        return project(input, vars, rowClass, 0L);
    }

    public static <R> FilterPlan<R> filter(Plan<R> input,
                                           Collection<? extends CharSequence> filters,
                                           Class<? super R> rowClass, long flags) {
        return new FilterPlan<>(create(Filter.class, flags, rowClass), input, filters);
    }
    public static <R> FilterPlan<R> filter(Plan<R> input,
                                           Collection<? extends CharSequence> filters,
                                           Class<? super R> rowClass) {
        return filter(input, filters, rowClass, 0L);
    }

    public static <R> FilterExistsPlan<R> filterExists(Plan<R> input, boolean negate,
                                                       Plan<R> filter, Class<? super R> rowClass,
                                                       long flags) {
        return new FilterExistsPlan<>(create(FilterExists.class, flags, rowClass), input, negate, filter);
    }
    public static <R> FilterExistsPlan<R> filterExists(Plan<R> input, boolean negate,
                                                       Plan<R> filter, Class<? super R> rowClass) {
        return filterExists(input, negate, filter, rowClass, 0L);
    }

    public static <R> MinusPlan<R> minus(Plan<R> left, Plan<R> right, Class<? super R> rowClass,
                                         long flags) {
        return new MinusPlan<>(create(Minus.class, flags, rowClass), left, right);
    }
    public static <R> MinusPlan<R> minus(Plan<R> left, Plan<R> right, Class<? super R> rowClass) {
        return minus(left, right, rowClass, 0L);
    }
}
