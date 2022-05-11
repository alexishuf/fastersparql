package com.github.alexishuf.fastersparql.client.util.sparql;

import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class AbstractBinding implements Binding {
    private final String[] vars;

    public AbstractBinding(String[] vars) {
        this.vars = vars;
    }

    @Override public final int size() {
        return vars.length;
    }

    @Override public final String var(int i) {
        return vars[i];
    }

    @Override public int indexOf(String var) {
        if (var == null) {
            assert false : "var == null";
            return -1;
        }
        for (int i = 0; i < vars.length; i++) {
            if (var.equals(vars[i]))
                return i;
        }
        return -1;
    }

    @Override public boolean contains(String var) {
        return indexOf(var) >= 0;
    }

    @Override public @Nullable String get(String var) {
        int i = indexOf(var);
        return i < 0 ? null : get(i);
    }

    @Override public Binding set(String var, @Nullable String value) {
        for (int i = 0; i < vars.length; i++) {
            if (var.equals(vars[i]))
                return set(i, value);
        }
        throw new IllegalArgumentException("var="+var+" is not present in "+this);
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder().append('{');
        for (int i = 0; i < vars.length; i++)
            sb.append(vars[i]).append('=').append(get(i)).append(", ");
        sb.setLength(Math.max(1, sb.length()-2));
        return sb.append('}').toString();
    }
}
