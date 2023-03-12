package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface JoinReorderStrategy extends NamedService<String> {
    /**
     * Reorder the {@code operands} array in-place. If any order change does happen, the
     * method will return {@code new Join(operands).publicVars()} computed according to the
     * original order of {@code operands}.
     *
     * @param operands the list of operands participating in the join.
     * @return {@code null} if ordering of operands was not changed, or
     *         {@code new Join(operands).publicVars()} using the original ordering.
     */
    @Nullable Vars reorder(Plan[] operands);

    /**
     * If {@code left ⋈ right} should be executed as {@code right ⋈ left}, returns
     * {@code left.publicVars().union(right)}, else returns {@code null}.
     */
    @Nullable Vars reorder(Plan left, Plan right);

    /**
     * The name under which this {@link JoinReorderStrategy} shall be selected. This should be
     * a unique name among all implementations, but shorter than a fully qualified class name,
     * allowing its use in configuration options and environment variables.
     *
     * @return a non-null, non-empty string unique among {@link JoinReorderStrategy}
     *         implementing classes.
     */
    default String name() { return getClass().getSimpleName().replace("JoinReorderStrategy", ""); }

    NamedServiceLoader<JoinReorderStrategy, String> NAMED_SERVICE_LOADER = new NamedServiceLoader<>(JoinReorderStrategy.class) {
        @Override protected boolean matches(String required, String offer) {
            return offer.trim().replace("_", "").replace("-", "").equalsIgnoreCase(required);
        }
        @Override protected JoinReorderStrategy fallback(String name) {
            return null;
        }
    };

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
        return NAMED_SERVICE_LOADER.get(name.trim().replace("_", "").replace("-", ""));
    }
}
