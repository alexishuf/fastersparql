package com.github.alexishuf.fastersparql.util;


import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

public class NamedServiceLoader<T extends NamedService<N>, N> {
    private final Class<T> cls;
    private Map<N, T> instances = Map.of();

    public NamedServiceLoader(Class<T> cls) {
        this.cls = cls;
    }

    protected boolean matches(N required, N offer) {
        if (required instanceof String r && offer instanceof String o)
            return r.equalsIgnoreCase(o);
        return required.equals(offer);
    }

    protected T fallback(N name) {
        throw new NoSuchElementException("No service provider named "+name);
    }

    public boolean has(N name) {
        return tryGet(name) != null;
    }

    public T get(N name) {
        T svc = tryGet(name);
        return svc == null ? fallback(name) : svc;
    }

    public @Nullable T tryGet(N name) {
        for (int i = 0; i < 2; i++, reload()) {
            T svc = instances.get(name);
            if (svc != null)
                return svc;
            for (N offer : instances.keySet()) {
                if (matches(name, offer)) return instances.get(offer);
            }
        }
        return null;
    }

    public Set<N> names() {
        if (instances.isEmpty())
            reload();
        return instances.keySet();
    }

    private void reload() {
        Map<N, T> chosen = new HashMap<>();
        for (T svc : ServiceLoader.load(cls)) {
            N name = svc.name();
            T current = chosen.get(name);
            int order = current == null ? Integer.MAX_VALUE : current.order();
            if (svc.order() < order)
                chosen.put(name, svc);
        }
        instances = chosen;
    }
}
