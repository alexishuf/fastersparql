package com.github.alexishuf.fastersparql.operators;


import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.impl.ProjectingProcessor;
import com.github.alexishuf.fastersparql.operators.plan.JoinPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.github.alexishuf.fastersparql.client.util.sparql.VarUtils.hasIntersection;
import static java.util.Arrays.asList;


/**
 * Utilities for implementers of Join
 */
public class JoinHelpers {

    /**
     * Executes the n-ary join using a left-deep tree (aka. left-associative execution).
     *
     * <p>The operators are optionally reordered before the execution tree is built, using the
     * given {@link JoinReorderStrategy}, which may be null</p>
     *
     * <p>Optionally Reorders {@code operands} and create a left-deep execution tree
     * (corresponding to a left-associative execution order).</p>
     *
     * @param plan A {@link JoinPlan} with unordered operands. It will not be modified
     * @param reorder The {@link JoinReorderStrategy} that will be used to reorder
     *                {@code plan.operands()} seeking lower execution time for full consumption
     *                of results. If null, the operands will not be reordered.
     * @param useBind whether the joins will use {@link Plan#bind(Binding)} this is forwarded to
     *                {@link JoinReorderStrategy#reorder(List, boolean)}.
     * @param binaryExecutor A function that turns a {@link JoinPlan} with two operands into a
     *                       {@link Results} instance.
     * @param <R> the row type
     * @return a {@link Results} over the results of the n-ary join.
     */
    public static <R> Results<R>
    executeReorderedLeftAssociative(JoinPlan<R> plan, @Nullable JoinReorderStrategy reorder,
                                    boolean useBind,
                                    Function<JoinPlan<R>, Results<R>> binaryExecutor) {
        List<? extends Plan<R>> inOps = plan.operands();
        List<? extends Plan<R>> ops = reorder == null ? inOps : reorder.reorder(inOps, useBind);
        switch (ops.size()) {
            case 0: return Results.empty(plan.rowClass());
            case 1: return ops.get(0).execute();
            case 2: return binaryExecutor.apply(reorder == null ? plan : plan.withOperands(ops));
        }

        int i0 = planIndex(ops.get(0), inOps), i1 = planIndex(ops.get(1), inOps);
        Plan<R> root = plan.op().<R>asPlan().operands(asList(ops.get(0), ops.get(1)))
                                .name(plan.name()+"["+i0+","+i1+"]").build();
        for (int i = 2; i < ops.size(); i++) {
            int oIdx = planIndex(ops.get(i), inOps);
            root = plan.op().<R>asPlan().operands(asList(root, ops.get(i)))
                            .name(plan.name()+"[*,"+oIdx+"]").build();
        }
        Results<R> results = root.execute();
        List<String> expectedVars = plan.publicVars();
        if (ops != plan.operands() && !root.publicVars().equals(expectedVars)) {
            Class<? super R> rowClass = results.rowClass();
            RowOperations ro = RowOperationsRegistry.get().forClass(rowClass);
            ProjectingProcessor<R> processor = new ProjectingProcessor<>(results, expectedVars, ro, null);
            results = new Results<>(expectedVars, rowClass, processor);
        }
        return results;
    }

    private static int planIndex(Plan<?> plan, List<? extends Plan<?>> list) {
        for (int i = 0, size = list.size(); i < size; i++) {
            Plan<?> candidate = list.get(i);
            if (candidate == plan)
                return i;
        }
        return -1;
    }

    private static final Pattern JRS_SUFFIX = Pattern.compile("joinreorderstrategy$");

    /**
     * Get the first {@link JoinReorderStrategy} known by the given name.
     *
     * <p>Name comparison is case-insensitive and spaces are trimmed. If no
     * {@link JoinReorderStrategy#name()} matches, will try to match {@code name} as a suffix of
     * the Fully Qualified Class Name (FQCN). When comparing with the class name, will additionally
     * try dropping the {@code JoinReorderStrategy} suffix of the class, if present.</p>
     *
     * @param name the {@link JoinReorderStrategy#name()}  or FQCN suffix
     * @return the first matching {@link JoinReorderStrategy} or null if no match was found.
     */
    public static @Nullable JoinReorderStrategy loadStrategy(String name) {
        name = name.trim();
        List<JoinReorderStrategy> list = new ArrayList<>();
        for (JoinReorderStrategy s : ServiceLoader.load(JoinReorderStrategy.class)) {
            assert s.name().trim().equals(s.name()) : "non-trimmed JoinReorderStrategy name";
            if (s.name().equalsIgnoreCase(name))
                return s;
            list.add(s);
        }
        String lowerName = name.toLowerCase();
        for (JoinReorderStrategy s : list) {
            String clsName = s.getClass().getName().toLowerCase();
            if (clsName.endsWith(lowerName))
                return s;
            if (JRS_SUFFIX.matcher(clsName).replaceFirst("").endsWith(lowerName))
                return s;
        }
        return null;
    }

    /**
     * Whether joining the two given operands would yield a cartesian product.
     *
     * @param leftVars public (i.e. result) vars of the left operand
     * @param right right-side operand
     * @param useBind whether the join will use the {@link Plan#bind(Binding)} operation.
     * @return true iff the join would cause a cartesian product.
     */
    public static boolean isProduct(Collection<String> leftVars, Plan<?> right, boolean useBind) {
        return !hasIntersection(leftVars, useBind ? right.allVars() : right.publicVars());
    }
}
