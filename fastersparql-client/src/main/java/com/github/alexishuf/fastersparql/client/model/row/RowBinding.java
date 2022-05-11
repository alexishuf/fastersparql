package com.github.alexishuf.fastersparql.client.model.row;

import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class RowBinding<R> implements Binding {
    private final RowOperations rowOps;
    private final List<String> vars;
    private @Nullable R row;

    public RowBinding(RowOperations rowOps, List<String> vars) {
        this.rowOps = rowOps;
        this.vars = vars;
    }

    public RowOperations rowOps() { return rowOps; }

    public List<String> vars() { return vars; }

    public RowBinding<R> row(@Nullable R row) {
        this.row = row;
        return this;
    }

    @Override public int     size()               { return vars.size();}
    @Override public String  var(int i)           { return vars.get(i); }
    @Override public int     indexOf(String var)  { return vars.indexOf(var); }
    @Override public boolean contains(String var) { return vars.contains(var); }
    @Override public @Nullable String get(int i)  { return rowOps.getNT(row, i, vars.get(i)); }

    @Override public @Nullable String get(String var) {
        int idx = vars.indexOf(var);
        return idx < 0 ? null : rowOps.getNT(row, idx, var);
    }

    @Override public Binding set(int i, @Nullable String value) {
        if (row == null) {
            //noinspection unchecked
            row = (R) rowOps.createEmpty(vars);
        }
        rowOps.set(row, i, vars.get(i), value);
        return this;
    }
    @Override public Binding set(String var, @Nullable String value) {
        if (row == null) {
            //noinspection unchecked
            row = (R) rowOps.createEmpty(vars);
        }
        rowOps.set(row, vars.indexOf(var), var, value);
        return this;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder().append('{');
        for (int i = 0; i < size(); i++)
            sb.append(var(i)).append('=').append(get(i)).append(", ");
        sb.setLength(Math.max(1, sb.length()-2));
        return sb.append('}').toString();
    }
}
