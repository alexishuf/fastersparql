package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.sparql.expr.TermParser;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public abstract class Binding {
    public final Vars vars;

    protected Binding(Vars vars) { this.vars = vars; }

    public final Vars vars() { return vars; }

    /** Get the number of vars in this binding */
    public final int size() { return vars.size(); }

    /**
     * Get the RDF term bound to the given var, in N-Triples syntax.
     *
     * @param var the var name, without leading '?'.
     * @return {@code null} if {@code var} is not mapped in this {@link Binding}, else the mapped
     *         N-Triples representation of an RDF term, which may also be {@code null}
     */
    public final @Nullable String get(String var) {
        int i = vars.indexOf(var);
        return i < 0 ? null : get(i);
    }

    /**
     * Get the RDF term bound to the given var as an {@link Term} instance. If it is not
     * already stored as a {@link Term} parse its N-Triples representation using the given
     * {@link TermParser}.
     *
     * @param var the var name
     * @return A {@link Term} or {@code null} if var is not mapped or mapped to null.
     */
    public @Nullable Term parse(String var) {
        int i = vars.indexOf(var);
        if (i < 0)
            return null;
        String nt = get(i);
        return nt == null ? null : TermParser.parse(nt);
    }

    /**
     * Equivalent to {@code get(var(i)}.
     *
     * @param i index of the value to fetch.
     * @return Mapped N-Triples representation of the value bound to the i-th var.
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= size()}
     */
    public abstract @Nullable String get(int i);

    /**
     * Maps the {@code i}-th variable to {@code null}
     *
     * @param i the index of the value to be changed
     * @return this {@link Binding}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    public final Binding clear(int i) { return set(i, null); }

    /**
     * Maps {@code var} to {@code null}
     *
     * @param var the variable whose value is to be removed.
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is unknown to this {@link Binding}
     */
    public final Binding clear(String var) { return set(var, null); }

    /**
     * Maps the {@code i}-th variable to {@code value}.
     *
     * @param i the index of the variable to update
     * @param value the new value for the {@code i}-th var.
     * @return this {@link Binding}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    public abstract Binding set(int i, @Nullable String value);

    /**
     * Maps {@code var} to {@code value}.
     *
     * @param var The var whose value will be updated.
     * @param value The new value for {@code var}
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is not known to this {@link Binding}
     */
    public final Binding set(String var, @Nullable String value) {
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
            String nt = get(i);
            hash = 31*hash + (nt == null ? 0 : nt.hashCode());
        }
        return hash;
    }
}
