package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.ServiceLoader;

public interface JoinReorderStrategy {
    /**
     * Returns a copy of the {@code operands} list, reordered for better faster enumeration of
     * all rows in a left-deep (left associative) execution of the operands.
     *
     * @param operands the list of operands participating in the join. This list will not
     *                 be mutated.
     * @param <P> an instantiation of the Plan generic interface
     * @return a new list with operands reordered.
     */
    <P extends Plan<?, ?> > List<P> reorder(List<P> operands, boolean usesBind);

    /**
     * The name under which this {@link JoinReorderStrategy} shall be selected. This should be
     * a unique name among all implementations, but shorter than a fully qualified class name,
     * allowing its use in configuration options and environment variables.
     *
     * @return a non-null, non-empty string unique among {@link JoinReorderStrategy}
     *         implementing classes.
     */
    default String name() { return getClass().getSimpleName().replace("JoinReorderStrategy", ""); }

    /**
     * Get the first {@link JoinReorderStrategy} with given {@link JoinReorderStrategy#name()}.
     *
     * <p>The name comparison is case-insensitive. {@code '-'}, {@code '_'} and {@code ' '}
     * are removed from {@link JoinReorderStrategy#name()} and {@code name} before comparison.</p>
     *
     * @param name the {@link JoinReorderStrategy#name()}
     * @return the first matching {@link JoinReorderStrategy} or {@code null} if no match was found
     */
    static @Nullable JoinReorderStrategy loadStrategy(String name) {
        name = name.trim().replace("_", "").replace("-", "");
        for (var s : ServiceLoader.load(JoinReorderStrategy.class)) {
            String candidate = s.name().trim().replace("_", "").replace("-", "");
            if (candidate.equalsIgnoreCase(name))
                return s;
        }
        return null;
    }
}
