package com.github.alexishuf.fastersparql.model;

/**
 * Bind semantics.
 *
 * <p>Let:</p>
 * <ul>
 *     <li>{@code bindings} denote a {@code Stream<R>} of rows given as bindings.</li>
 *     <li>{@code template::bind} denote a {@code Function<Stream<R>, Stream<R>>} that executes
 *         a template query with variables replaced by values given by a row from
 *         {@code bindings}
 *    </li>
 *    <li>{@code emptyRow} denote an empty row (a row with zero columns)</li>
 * </ul>
 *
 * <p>Then, suing the Java {@link java.util.stream.Stream} API, the semantics for each bind type
 * is defined below.</p>
 */
public enum BindType {
    /**
     * Equivalent to {@code bindings.flatMap(template::bind)}.
     *
     * <p>(See {@link BindType} for definitions of the symbols used)</p>
     */
    JOIN,

    /**
     * Equivalent to {@code bindings.flatMap(r -> {
     *     List<R> list = template.bind(r).collect(toList());
     *     return list.isEmpty() ? Stream.of(emptyRow) : list.stream();
     * })}
     *
     * <p>(See {@link BindType} for definitions of the symbols used)</p>
     */
    LEFT_JOIN,

    /**
     * Equivalent to {@code bindings.filter(r -> template.bind(r).count() > 0)}
     *
     * <p>(See {@link BindType} for definitions of the symbols used)</p>
     */
    EXISTS,

    /**
     * Equivalent to {@code bindings.filter(r -> template.bind(r).count() == 0)}
     *
     * <p>(See {@link BindType} for definitions of the symbols used)</p>
     */
    NOT_EXISTS,

    /**
     * Equivalent to {@link BindType#NOT_EXISTS} with two exceptions described in
     * <a href="specification">Section 8.3 of the SPARQL specification</a>:
     *
     * <ol>
     *     <li>{@code template.bind(r)} will only replaces variables in the template if they
     *         occur at least once outside of a FILTER expression in the template.</li>
     *     <li>If the template query does not share a variable with the bindings result
     *         set outside of the template's FILTER expressions, MINUS will be a no-op in the
     *         sense that no input bindings will be eliminated.</li>
     * </ol>
     */
    MINUS;

    public Vars resultVars(Vars binding, Vars template) {
        return this == JOIN || this == LEFT_JOIN ? binding.union(template) : binding;
    }

    public boolean isJoin() { return this == JOIN || this == LEFT_JOIN; }
}
