package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collection;

public class ArrayBinding extends Binding {
    public static final ArrayBinding EMPTY = new ArrayBinding(Vars.EMPTY, new Term[0]);

    private final @Nullable Term[] values;

    public ArrayBinding(Vars vars) {
        super(vars);
        this.values = new Term[vars.size()];
    }

    public ArrayBinding(Vars vars, @Nullable Binding parent) {
        super(vars);
        this.values = new Term[vars.size()];
        if (parent != null) {
            for (int i = 0; i < this.values.length; i++) {
                SegmentRope name = vars.get(i);
                values[i] = parent.get(name);
            }
        }
    }

    public ArrayBinding(Vars vars, Term[] values) {
        super(vars);
        this.values = values;
    }

    public ArrayBinding(Vars vars, Collection<Term> values) {
        super(vars);
        this.values = values.toArray(Term[]::new);
    }

    public static ArrayBinding of(CharSequence... varAndValues) {
        if ((varAndValues.length & 1) == 1)
            throw new IllegalArgumentException("Expected even length for varAndValues");
        var vars = new Vars.Mutable(varAndValues.length >> 1);
        var terms = new Term[varAndValues.length>>1];
        for (int i = 0; i < varAndValues.length; i += 2) {
            var name = SegmentRope.of(varAndValues[i]);
            if (name.len() > 0 && (name.get(0) == '?' || name.get(0) == '$'))
                name = name.sub(1, name.len());
            if (name.len() == 0)
                throw new IllegalArgumentException("Empty string is not a valid var name");
            vars.add(name);
            terms[i>>1] = Term.array(varAndValues[i+1])[0];
        }
        return new ArrayBinding(vars, terms);
    }

    @Override public @Nullable Term get(int i) {
        return values[i];
    }

    @Override public Binding set(int i, @Nullable Term value) {
        values[i] = value;
        return this;
    }

    @Override public void clear() { Arrays.fill(values, null); }

    public Term[] copyValues() { return Arrays.copyOf(values, values.length); }

    public void setValues(Term[] arr) {
        System.arraycopy(arr, 0, values, 0, arr.length);
    }
}
