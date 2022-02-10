package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.List;

public interface JoinReorderStrategy {
    /**
     * Returns a copy of the {@code operands} list, reodered for better faster enumeration of
     * all rows in a left-deep (left associative) execution of the operands.
     *
     * @param operands the list of operands participating in the join. This list will not
     *                 be mutated.
     * @param <P> an instantiation of the Plan generic interface
     * @return a new list with operands reordered.
     */
    <P extends Plan<?> > List<P> reorder(List<P> operands, boolean usesBind);

    /**
     * The name under which this {@link JoinReorderStrategy} shall be selected. This should be
     * an unique name among all implementations, but shorter than a fully qualified class name,
     * allowing its use in configuration options and environment variables.
     *
     * @return a non-null, non-empty string unique among {@link JoinReorderStrategy}
     *         implementing classes.
     */
    String name();
}
