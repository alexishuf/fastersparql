package com.github.alexishuf.fastersparql.client.util.sparql;

import org.checkerframework.checker.nullness.qual.Nullable;

public interface Binding {
    /** Get the number of vars in this binding */
    int size();

    /**
     * Get the {@code i}-th var name
     *
     * @param i the var index, {@code >= 0} and {@code <} {@link Binding#size()}
     * @return The var name (wuthout leading '?').
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= } {@link Binding#size()}.
     */
    String var(int i);

    /**
     * Gets the {@code i} such that {@code var(i).equals(var)} or -1.
     *
     * @param var the var name, without preceding '?'
     * @return the index of the var in this {@link Binding} or -1 if it is not present.
     */
    int indexOf(String var);

    /**
     * Checks if var is present in this {@link Binding}, even if mapped to {@code null}.
     *
     * @param var the var name, without a preceding '?'
     * @return {@code true} iff {@code indexOf(var) >= 0}.
     */
    boolean contains(String var);

    /**
     * Get the RDF term bound to the given var, in N-Triples syntax.
     *
     * @param var the var name, without leading '?'.
     * @return {@code null} if {@code var} is not mapped in this {@link Binding}, else the mapped
     *         N-Triples representation of an RDF term, which may also be {@code null}
     */
    @Nullable String get(String var);

    /**
     * Equivalent to {@code get(var(i)}.
     *
     * @param i index of the value to fetch.
     * @return Mapped N-Triples representation of the value bound to the i-th var.
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= size()}
     */
    @Nullable String get(int i);

    /**
     * Maps the {@code i}-th variable to {@code null}
     *
     * @param i the index of the value to be changed
     * @return this {@link Binding}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    default Binding clear(int i) { return set(i, null); }

    /**
     * Maps {@code var} to {@code null}
     *
     * @param var the variable whose value is to be removed.
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is unknown to this {@link Binding}
     */
    default Binding clear(String var) { return set(var, null); }

    /**
     * Maps the {@code i}-th variable to {@code value}.
     *
     * @param i the index of the variable to update
     * @param value the new value for the {@code i}-th var.
     * @return this {@link Binding}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    Binding set(int i, @Nullable String value);

    /**
     * Maps {@code var} to {@code value}.
     *
     * @param var The var whose value will be updated.
     * @param value The new value for {@code var}
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is not known to this {@link Binding}
     */
    Binding set(String var, @Nullable String value);
}
