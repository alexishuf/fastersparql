package com.github.alexishuf.fastersparql.operators.row;

public interface RowMatcher {
    /**
     * Whether this {@link RowMatcher} was created for two variable sets that intersect.
     *
     * If this is false, {@link RowMatcher#matches(Object, Object)} will always return {@code true}.
     *
     * @return whether variable sets given at creation time intersect.
     */
    boolean hasIntersection();

    /**
     * Whether for all variables shared between left and right, the values are equals.
     *
     * If there are no shared variables, this will still return {@code true}.
     * See {@link RowMatcher#hasIntersection()}.
     *
     * For value (i.e., a term in a row) comparison purposes, two {@code null}s are equals.
     *
     * @param left left row
     * @param right right row
     * @return {@code true} iff for all shared variables, values are equals.
     */
    boolean matches(Object left, Object right);
}
