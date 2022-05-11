package com.github.alexishuf.fastersparql.client.util.sparql;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

public class ArrayBinding extends AbstractBinding {
    private @Nullable String[] values;
    private @Nullable String[] owned;

    public ArrayBinding(String[] vars) {
        super(vars);
        this.values = this.owned = new String[vars.length];
    }

    public ArrayBinding(List<String> vars) {
        this(vars.toArray(new String[0]));
    }

    private ArrayBinding(String[] vars, String[] values) {
        super(vars);
        this.values = values;
        this.owned = null;
    }

    public static ArrayBinding wrap(String[] vars, String @Nullable[] values) {
        return new ArrayBinding(vars, values == null ? new String[vars.length] : values);
    }

    public static ArrayBinding wrap(String[] vars, CharSequence @Nullable[] values) {
        ArrayBinding binding = new ArrayBinding(vars);
        return values == null ? binding : binding.values(values);
    }

    public static ArrayBinding copy(Map<String, @Nullable String> var2value) {
        int size = var2value.size();
        String[] vars = new String[size], values = new String[size];
        int i = 0;
        for (Map.Entry<String, String> e : var2value.entrySet()) {
            vars[i] = e.getKey();
            values[i++] = e.getValue();
        }
        ArrayBinding binding = new ArrayBinding(vars, values);
        binding.owned = values;
        return binding;
    }

    public ArrayBinding values(@Nullable String[] values) {
        if (values.length != size())
            throw new IllegalArgumentException("Expected "+size()+" values, got "+values.length);
        this.values = values;
        return this;
    }

    public ArrayBinding values(@Nullable CharSequence[] values) {
        if (values.length != size())
            throw new IllegalArgumentException("Expected "+size()+" values, got "+values.length);
        if (this.owned == null)
            this.owned = new String[size()];
        this.values = this.owned;
        int i = 0;
        for (CharSequence cs : values)
            this.values[i++] = cs == null ? null : cs.toString();
        return this;
    }

    @Override public @Nullable String get(int i) {
        return values[i];
    }

    @Override public Binding set(int i, @Nullable String value) {
        values[i] = value;
        return this;
    }
}
