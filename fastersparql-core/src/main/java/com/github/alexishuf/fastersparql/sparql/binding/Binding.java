package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.PrefixAssigner;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public abstract class Binding {

    public abstract Vars vars();

    /** Get the number of vars in this binding */
    public final int size() { return vars().size(); }

    /** Get the {@link Term} bound to the given var or {@code null} if var is not bound. */
    public final @Nullable Term get(SegmentRope name) {
        int i = vars().indexOf(name);
        return i < 0 ? null : get(i);
    }

    /** Get the {@link Term} bound to the given var or {@code null} if var is not bound. */
    public final @Nullable Term get(Term var) {
        int i = vars().indexOf(var);
        return i < 0 ? null : get(i);
    }

    /**
     * Equivalent to {@code get(name) != null}, but may avoid instantiating a {@link Term}.
     */
    public final boolean has(SegmentRope name) {
        int i = vars().indexOf(name);
        return i >= 0 && has(i);
    }

    /**
     * Equivalent to {@code get(var) != null}, but may avoid instantiating a {@link Term}.
     */
    public final boolean has(Term var) {
        int i = vars().indexOf(var);
        return i >= 0 && has(i);
    }

    /**
     * Equivalent to
     * <pre>{@code
     *     Term term = get(i);
     *     if (term != null)
     *         term.toSparql(dest, prefixAssigner);
     * }</pre>
     *
     * @param i column to be fetched in this {@link Binding}
     * @param dest see {@link Term#toSparql(ByteSink, PrefixAssigner)}
     * @param prefixAssigner see {@link Term#toSparql(ByteSink, PrefixAssigner)}
     */
    public int writeSparql(int i, ByteSink<?, ?> dest, PrefixAssigner prefixAssigner) {
        Term term = get(i);
        return term == null ? 0 : term.toSparql(dest, prefixAssigner);
    }

    /** If {@code term} is var that is bound, get the value, else return {@code term} itself. */
    public final Term getIf(Term term) {
        int i = vars().indexOf(term);
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

    /** Equivalent to {@code get(i) != null}, but may avoid instantiating {@link Term}. */
    public boolean has(int i) { return get(i) != null; }

    /* --- --- java.lang.Object methods --- --- --- */

    @Override public final String toString() {
        var sb = new ByteRope().append('{');
        Vars vars = vars();
        int n = vars.size();
        for (int i = 0; i < n; i++) {
            Term t = get(i);
            sb.append(vars.get(i)).append('=').append(t==null ? "null" : t.toSparql()).append(", ");
        }
        if (n > 0)
            sb.unAppend(2);
        return sb.append('}').toString();
    }

    @Override public final boolean equals(Object other) {
        if (other == this) return true;
        if (!(other instanceof Binding b) || !vars().equals(b.vars())) return false;
        for (int i = 0, size = size(); i < size; i++) {
            if (!Objects.equals(get(i), b.get(i))) return false;
        }
        return true;
    }

    @Override public final int hashCode() {
        int hash = vars().hashCode();
        for (int i = 0, size = size(); i < size; i++) {
            var term = get(i);
            hash = 31*hash + (term == null ? 0 : term.hashCode());
        }
        return hash;
    }
}
