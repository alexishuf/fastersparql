package com.github.alexishuf.fastersparql.client.util.sparql;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MapBinding extends AbstractBinding {
    private Map<String, @Nullable String> map;

    public MapBinding(String[] vars) {
        super(vars);
        this.map = Collections.emptyMap();
    }

    private MapBinding(String[] vars, Map<String, @Nullable String> map) {
        super(vars);
        this.map = map;
    }

    public static MapBinding wrap(String[] vars, Map<String, @Nullable String> map) {
        return new MapBinding(vars, map);
    }

    public static MapBinding wrap(List<String> vars, Map<String, @Nullable String> map) {
        return new MapBinding(vars.toArray(new String[0]), map);
    }

    public static MapBinding wrap(Map<String, @Nullable String> map) {
        String[] vars = new String[map.size()];
        int i = 0;
        for (String name : map.keySet())
            vars[i++] = name;
        return new MapBinding(vars, map);
    }

    public MapBinding values(Map<String, @Nullable String> map) {
        this.map = map;
        return this;
    }

    @Override public boolean contains(String var) {
        return map.containsKey(var);
    }

    @Override public @Nullable String get(String var) {
        return map.getOrDefault(var, null);
    }

    @Override public @Nullable String get(int i) {
        return map.getOrDefault(var(i), null);
    }

    @Override public Binding set(int i, @Nullable String value) {
        map.put(var(i), value);
        return this;
    }

    @Override public Binding set(String var, @Nullable String value) {
        if (!map.containsKey(var))
            throw new IllegalArgumentException("var="+var+" unknown to "+this);
        map.put(var, value);
        return this;
    }
}
