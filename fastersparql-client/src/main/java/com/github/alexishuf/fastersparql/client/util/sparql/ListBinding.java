package com.github.alexishuf.fastersparql.client.util.sparql;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ListBinding extends AbstractBinding {
    private List<String> values;
    private ArrayList<String> owned;

    public ListBinding(String[] vars) {
        super(vars);
        this.values = this.owned = new ArrayList<>(vars.length);
        for (int i = 0; i < vars.length; i++)
            values.add(null);
    }

    public ListBinding(List<String> vars) {
        this(vars.toArray(new String[0]));
    }

    private ListBinding(String[] vars, List<? extends CharSequence> values) {
        super(vars);
        values(values);
    }

    public static ListBinding wrap(String[] vars, @Nullable List<? extends CharSequence> values) {
        return values == null ? new ListBinding(vars) : new ListBinding(vars, values);
    }

    public static ListBinding wrap(List<String> vars, @Nullable List<? extends CharSequence> values) {
        return values == null ? new ListBinding(vars)
                              : new ListBinding(vars.toArray(new String[0]), values);
    }

    public static ListBinding copy(Map<String, @Nullable String> var2value) {
        int size = var2value.size();
        String[] vars = new String[size];
        ListBinding binding = new ListBinding(vars);
        int i = 0;
        for (Map.Entry<String, String> e : var2value.entrySet()) {
            vars[i] = e.getKey();
            binding.values.set(i++, e.getValue());
        }
        return binding;
    }

    public ListBinding values(List<? extends CharSequence> values) {
        if (values.size() != size())
            throw new IllegalArgumentException("Expected "+size()+" terms, got "+values.size());
        for (CharSequence v : values) {
            if (v != null && !(v instanceof String)) {
                if (owned == null) {
                    owned = new ArrayList<>(values.size());
                } else {
                    owned.clear();
                    owned.ensureCapacity(values.size());
                }
                for (CharSequence cs : values)
                    owned.add(cs == null ? null : cs.toString());
                values = owned;
                break;
            }
        }
        //noinspection unchecked
        this.values = (List<String>) values;
        return this;
    }

    @Override public @Nullable String get(int i) {
        return values.get(i);
    }

    @Override public Binding set(int i, @Nullable String value) {
        values.set(i, value);
        return this;
    }
}
