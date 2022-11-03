package com.github.alexishuf.fastersparql.sparql.binding;

import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public class ArrayBinding extends Binding {
    public static final ArrayBinding EMPTY = new ArrayBinding(Vars.EMPTY, new String[0]);

    private @Nullable String[] values;
    private @Nullable String[] owned;

    public ArrayBinding(Vars vars) {
        super(vars);
        this.values = this.owned = new String[vars.size()];
    }

    private ArrayBinding(Vars vars, String[] values) {
        super(vars);
        this.values = values;
        this.owned = null;
    }

    public static ArrayBinding wrap(Vars vars, String @Nullable[] values) {
        return new ArrayBinding(vars, values == null ? new String[vars.size()] : values);
    }

    public static ArrayBinding wrap(Vars vars, CharSequence @Nullable[] values) {
        ArrayBinding binding = new ArrayBinding(vars);
        return values == null ? binding : binding.values(values);
    }

    public static ArrayBinding copy(Map<String, @Nullable String> var2value) {
        int size = var2value.size();
        Vars vars = Vars.fromSet(var2value.keySet());
        String[] values = new String[size];
        int i = 0;
        for (String value : var2value.values())
            values[i++] = value;
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
