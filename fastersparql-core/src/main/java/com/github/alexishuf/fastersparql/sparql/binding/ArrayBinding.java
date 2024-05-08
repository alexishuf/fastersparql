package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Arrays;
import java.util.Collection;

import static java.lang.System.arraycopy;

public class ArrayBinding extends Binding {
    public static final int BYTES = 16 + 4*2 + 20;
    public static final ArrayBinding EMPTY = new ArrayBinding(Vars.EMPTY, new Term[0]);

    public Vars vars;
    private @Nullable Term[] values;

    public ArrayBinding(Vars vars) {
        this.vars = vars;
        this.values = new Term[vars.size()];
    }

    public ArrayBinding(Vars vars, @Nullable Binding parent) {
        this.vars = vars;
        this.values = new Term[vars.size()];
        if (parent != null) {
            for (int i = 0; i < this.values.length; i++) {
                SegmentRope name = vars.get(i);
                values[i] = parent.get(name);
            }
        }
    }

    public ArrayBinding(Vars vars, Term[] values) {
        this.vars = vars;
        this.values = values;
    }

    public ArrayBinding(Vars vars, Collection<Term> values) {
        this.vars = vars;
        this.values = values.toArray(Term[]::new);
    }

    public static ArrayBinding of(CharSequence... varAndValues) {
        if ((varAndValues.length & 1) == 1)
            throw new IllegalArgumentException("Expected even length for varAndValues");
        var vars = new Vars.Mutable(varAndValues.length >> 1);
        var terms = new Term[varAndValues.length>>1];
        for (int i = 0; i < varAndValues.length; i += 2) {
            SegmentRope name = FinalSegmentRope.asFinal(varAndValues[i]);
            if (name.len == 0)
                throw new IllegalArgumentException("Empty string is not a valid var name");
            else if (name.get(0) == '?' || name.get(0) == '$')
                name = name.sub(1, name.len);
            vars.add(name);
            terms[i>>1] = Term.array(varAndValues[i+1])[0];
        }
        return new ArrayBinding(vars, terms);
    }

    public void reset(Vars vars) {
        this.vars = vars;
        int n = vars.size();
        if (values.length < n) values = new Term[n];
        else                   Arrays.fill(values, 0, n, null);
    }

    public Term[] swapValues(Term[] newValues) {
        if (newValues.length < vars.size())
            throw new IllegalArgumentException("newValues.length < vars.size()");
        @Nullable Term[] oldValues = this.values;
        this.values = newValues;
        return oldValues;
    }

    public void copyValuesInto(Term[] dst) {
        arraycopy(values, 0, dst, 0, vars.size());
    }

    @Override public Vars vars() { return vars; }

    @Override public @Nullable Term get(int i) { return values[i]; }

    @Override public boolean hasSpecialRef(int i, Term expected) {
        return values[i] == expected;
    }

    public boolean hasAll(Vars other) {
        Vars vars = this.vars;
        //noinspection ForLoopReplaceableByForEach
        for (int oIdx = 0, n = other.size(); oIdx < n; oIdx++) {
             int i = vars.indexOf(other.get(oIdx));
             if (i < 0 || values[i] == null) return false;
        }
        return true;
    }

    public boolean ground(Vars groundVars) {
        Vars vars = this.vars;
        boolean changes = false;
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0, n = groundVars.size(); i < n; i++) {
            int dst = vars.indexOf(groundVars.get(i));
            if (dst >= 0 && values[dst] == null) {
                changes = true;
                values[dst] = Term.GROUND;
            }
        }
        return changes;
    }

    /**
     * Maps the {@code i}-th variable to {@code null}
     *
     * @param i the index of the value to be changed
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    public final void clear(int i) { set(i, null); }

    /** Removes all mappings to values in this {@link Binding}, i.e., call {@code clear(i)}
     *  for every {@code i} in {@code [0,size())}. */
    public void clear() {
        Arrays.fill(values, null);
    }

    /**
     * Maps {@code var} to {@code null}
     *
     * @param var the variable whose value is to be removed.
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is unknown to this {@link Binding}
     */
    public final @This ArrayBinding clear(SegmentRope var) { return set(var, null); }

    /**
     * Maps the {@code i}-th variable to {@code value}.
     *
     * @param i the index of the variable to update
     * @param value the new value for the {@code i}-th var.
     * @return this {@link Binding}
     * @throws IndexOutOfBoundsException if {@code i < 0 || i > size()}.
     */
    public @This ArrayBinding set(int i, @Nullable Term value) {
        values[i] = value;
        return this;
    }

    /**
     * Maps {@code var} to {@code value}.
     *
     * @param var The var whose value will be updated.
     * @param value The new value for {@code var}
     * @return this {@link Binding}
     * @throws IllegalArgumentException if {@code var} is not known to this {@link Binding}
     */
    public final @This ArrayBinding set(SegmentRope var, @Nullable Term value) {
        int i = vars.indexOf(var);
        if (i == -1)
            throw new IllegalArgumentException("var="+var+" is not present in "+this);
        return set(i, value);
    }

    public final @This ArrayBinding set(Term var, @Nullable Term value) {
        int i = vars.indexOf(var);
        if (i == -1)
            throw new IllegalArgumentException("var="+var+" is not present in "+this);
        return set(i, value);
    }

}
