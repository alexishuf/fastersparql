package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;

import java.util.List;

/**
 * Bind semantics.
 *
 * Let:
 * <ul>
 *     <li>{@code bindings} denote a {@code Stream<R>} of rows given as bindings.</li>
 *     <li>{@code template::bind} denote a {@code Function<Stream<R>, Stream<R>>} that executes
 *         a template query with variables replaced by values given by a row from
 *         {@code bindings}
 *    </li>
 *    <li>{@code emptyRow} denote an empty row (a row with zero columns)</li>
 * </ul>
 *
 * Then, suing the Java {@link java.util.stream.Stream} API, the semantics for each bind type
 * is defined below.
 */
public enum BindType {
    /**
     * Equivalent to {@code bindings.flatMap(template::bind)}.
     *
     * (See {@link BindType} for definitions of the symbols used)
     */
    JOIN,

    /**
     * Equivalent to {@code bindings.flatMap(r -> {
     *     List<R> list = template.bind(r).collect(toList());
     *     return list.isEmpty() ? Stream.of(emptyRow) : list.stream();
     * })}
     *
     * (See {@link BindType} for definitions of the symbols used)
     */
    LEFT_JOIN,

    /**
     * Equivalent to {@code bindings.filter(r -> template.bind(r).count() > 0)}
     *
     * (See {@link BindType} for definitions of the symbols used)
     */
    EXISTS,

    /**
     * Equivalent to {@code bindings.filter(r -> template.bind(r).count() == 0)}
     *
     * (See {@link BindType} for definitions of the symbols used)
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

    public List<String> resultVars(List<String> bindingVars, List<String> templateVars) {
        return isJoin() ? VarUtils.union(bindingVars, templateVars) : bindingVars;
    }

    public boolean isJoin() { return this == JOIN || this == LEFT_JOIN; }
}
