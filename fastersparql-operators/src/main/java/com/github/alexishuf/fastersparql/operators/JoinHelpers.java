package com.github.alexishuf.fastersparql.operators;


import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.operators.impl.ProjectingProcessor;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.PlanHelpers;
import com.github.alexishuf.fastersparql.operators.reorder.JoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.reorder.NullJoinReorderStrategy;
import com.github.alexishuf.fastersparql.operators.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.row.RowOperationsRegistry;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static com.github.alexishuf.fastersparql.client.util.sparql.VarUtils.hasIntersection;


/**
 * Utilities for implementers of Join
 */
public class JoinHelpers {
    /**
     * Executes the n-ary join using a left-deep tree (aka. left-associative execution).
     *
     * The operators are optionally reordered before the execution tree is built, using the
     * given {@link JoinReorderStrategy}, which may be null
     *
     * Optionally Reorders {@code operands} and create a left-deep execution tree
     * (corresponding to a left-associative execution order).
     *
     * @param unorderedOperands the join operands, not yet ordered (this list will not be mutated).
     * @param joinReorderStrategy the {@link JoinReorderStrategy} that shall be used to
     *                            rearrange the given operands seeking better performance. If null,
     *                            the operands will not be reordered.
     * @param useBind whether the joins will use {@link Plan#bind(Map)} this is forwarded to
     *                {@link JoinReorderStrategy#reorder(List, boolean)}.
     * @param executor A binary function that combines a left-side {@link Results} to a right-side
     *                 {@link Plan} into a {@link Results} of the join results.
     * @param <R> the row type
     * @return a {@link Results} over the results of the n-ary join.
     */
    public static <R> Results<R>
    executeReorderedLeftAssociative(List<? extends Plan<R>> unorderedOperands,
                                    @Nullable JoinReorderStrategy joinReorderStrategy,
                                    boolean useBind,
                                    BiFunction<Results<R>, Plan<R>, Results<R>> executor) {
        if (unorderedOperands.isEmpty())
            return new Results<>(Collections.emptyList(), Object.class, new EmptyPublisher<>());
        if (unorderedOperands.size() == 1)
            return unorderedOperands.get(0).execute();
        if (joinReorderStrategy == null)
            joinReorderStrategy = NullJoinReorderStrategy.INSTANCE;
        List<String> expectedVars = PlanHelpers.publicVarsUnion(unorderedOperands);
        List<? extends Plan<R>> operands = joinReorderStrategy.reorder(unorderedOperands, useBind);
        Results<R> root = executor.apply(operands.get(0).execute(), operands.get(1));
        for (int i = 2, size = operands.size(); i < size; i++)
            root = executor.apply(root, operands.get(i));
        if (operands != unorderedOperands && !root.vars().equals(expectedVars)) {
            Class<? super R> rowClass = root.rowClass();
            RowOperations ro = RowOperationsRegistry.get().forClass(rowClass);
            ProjectingProcessor<R> processor = new ProjectingProcessor<>(root, expectedVars, ro);
            root = new Results<>(expectedVars, rowClass, processor);
        }
        return root;
    }

    private static final Pattern JRS_SUFFIX = Pattern.compile("joinreorderstrategy$");

    /**
     * Get the first {@link JoinReorderStrategy} known by the given name.
     *
     * Name comparison is case-insensitive and spaces are trimmed. If no
     * {@link JoinReorderStrategy#name()} matches, will try to match {@code name} as a suffix of
     * the Fully Qualified Class Name (FQCN). When comparing with the class name, will additionally
     * try dropping the {@code JoinReorderStrategy} suffix of the class, if present.
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
     * @param useBind whether the join will use the {@link Plan#bind(Map)} operation.
     * @return true iff the join would cause a cartesian product.
     */
    public static boolean isProduct(Collection<String> leftVars, Plan<?> right, boolean useBind) {
        return !hasIntersection(leftVars, useBind ? right.allVars() : right.publicVars());
    }
}
