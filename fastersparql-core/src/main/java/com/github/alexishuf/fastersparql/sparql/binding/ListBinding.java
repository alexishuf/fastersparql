package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ListBinding extends Binding {
    private List<String> values;
    private ArrayList<String> owned;

    public ListBinding(Vars vars) {
        super(vars);
        int size = vars.size();
        this.values = this.owned = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            values.add(null);
    }

    private ListBinding(Vars vars, List<? extends CharSequence> values) {
        super(vars);
        values(values);
    }

    public static ListBinding wrap(Vars vars, @Nullable List<? extends CharSequence> values) {
        return values == null ? new ListBinding(vars) : new ListBinding(vars, values);
    }

    public static ListBinding copy(Map<String, @Nullable String> var2value) {
        Vars vars = Vars.fromSet(var2value.keySet());
        return new ListBinding(vars).values(new ArrayList<>(var2value.values()));
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
