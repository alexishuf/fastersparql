package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public abstract class Binding {
    public final Vars vars;

    protected Binding(Vars vars) { this.vars = vars; }

    public final Vars vars() { return vars; }

    /** Get the number of vars in this binding */
    public final int size() { return vars.size(); }

    /** Get the {@link Term} bound to the given var or {@code null} if var is not bound. */
    public final @Nullable Term get(Rope varOrVarName) {
        int i = vars.indexOf(varOrVarName);
        return i < 0 ? null : get(i);
    }

    /** If {@code term} is var that is bound, get the value, else return {@code term} itself. */
    public final Term getIf(Term term) {
        int i = vars.indexOf(term);
        Term bound = i < 0 ? null : get(i);
        return bound == null ? term : bound;
    }

    /**
     * Equivalent to {@code get(var(i)}.
     *
     * @param i index of the value to fetch.
     * @return Mapped N-Triples representation of the value bound to the i-th var.
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= size()}
     */
    public abstract @Nullable Term get(int i);

    /** Whether no var in this binding has a value associated, i.e., {@code get(i)==null}
     *  for every {@code i} in {@code [0, size())}. */
    public final boolean isUnbound() {
        for (int i = 0, n = size(); i < n; i++) {
            if (get(i) != null) return false;
        }
        return true;
    }

    /**
     * Maps the {@code i}-th variable to {@code null}
     *
     * @param i the index of the value to be changed
     * @return this {@link Binding}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    public final Binding clear(int i) { return set(i, null); }

    /** Removes all mappings to values in this {@link Binding}, i.e., call {@code clear(i)}
     *  for every {@code i} in {@code [0,size())}. */
    public void clear() {
        for (int i = 0; i < size(); i++)
            clear(i);
    }

    /**
     * Maps {@code var} to {@code null}
     *
     * @param var the variable whose value is to be removed.
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is unknown to this {@link Binding}
     */
    public final Binding clear(Rope var) { return set(var, null); }

    /**
     * Maps the {@code i}-th variable to {@code value}.
     *
     * @param i the index of the variable to update
     * @param value the new value for the {@code i}-th var.
     * @return this {@link Binding}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    public abstract Binding set(int i, @Nullable Term value);

    /**
     * Maps {@code var} to {@code value}.
     *
     * @param var The var whose value will be updated.
     * @param value The new value for {@code var}
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is not known to this {@link Binding}
     */
    public final Binding set(Rope var, @Nullable Term value) {
        int i = vars.indexOf(var);
        if (i == -1)
            throw new IllegalArgumentException("var="+var+" is not present in "+this);
        return set(i, value);
    }

    /* --- --- java.lang.Object methods --- --- --- */

    @Override public final String toString() {
        var sb = new StringBuilder().append('{');
        for (int i = 0; i < vars.size(); i++)
            sb.append(vars.get(i)).append('=').append(get(i)).append(", ");
        sb.setLength(Math.max(1, sb.length()-2));
        return sb.append('}').toString();
    }

    @Override public final boolean equals(Object other) {
        if (other == this) return true;
        if (!(other instanceof Binding b) || !vars.equals(b.vars)) return false;
        for (int i = 0, size = size(); i < size; i++) {
            if (!Objects.equals(get(i), b.get(i))) return false;
        }
        return true;
    }

    @Override public final int hashCode() {
        int hash = vars.hashCode();
        for (int i = 0, size = size(); i < size; i++) {
            var term = get(i);
            hash = 31*hash + (term == null ? 0 : term.hashCode());
        }
        return hash;
    }
}
