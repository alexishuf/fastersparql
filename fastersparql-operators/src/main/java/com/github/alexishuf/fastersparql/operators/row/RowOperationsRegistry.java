package com.github.alexishuf.fastersparql.operators.row;

import com.github.alexishuf.fastersparql.operators.errors.NoRowOperationsException;
import com.github.alexishuf.fastersparql.operators.row.impl.NullRowOperations;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class RowOperationsRegistry {
    private static final RowOperationsRegistry INSTANCE = new RowOperationsRegistry().registerAll();

    private final Map<Class<?>, RowOperationsProvider> providerMap = new HashMap<>();

    public static RowOperationsRegistry get() { return INSTANCE; }

    /**
     * Register the given {@link RowOperationsProvider} for later use in the {@code for*} methods.
     * @param provider the {@link RowOperationsProvider}.
     */
    public void register(RowOperationsProvider provider) {
        providerMap.put(provider.rowClass(), provider);
    }

    /**
     * Register all {@link RowOperationsProvider} exposed via SPI ({@link ServiceLoader}).
     *
     * This method is not thread-safe.
     *
     * @return {@code this}.
     */
    public RowOperationsRegistry registerAll() {
        for (RowOperationsProvider provider : ServiceLoader.load(RowOperationsProvider.class))
            register(provider);
        return this;
    }

    /**
     * Get the most specific {@link RowOperations} that can handle the given class.
     *
     * @param cls the {@link Class} of rows
     * @return a non-null {@link RowOperations}
     * @throws NoRowOperationsException if there is no {@link RowOperationsProvider}
     *         compatible with the given {@code cls}
     */
    public RowOperations forClass(Class<?> cls) throws NoRowOperationsException {
        if (cls == null || cls.equals(Object.class)) return NullRowOperations.INSTANCE;
        for (Class<?> current = cls; current != null; current = current.getSuperclass()) {
            RowOperationsProvider p = providerMap.get(current);
            if (p != null) return p.get();
            for (Class<?> ifc : current.getInterfaces()) {
                if ((p = providerMap.get(ifc)) != null)
                    return p.get();
            }
        }
        throw new NoRowOperationsException(cls);
    }

    /**
     * Null-safe {@code forClass(row.getClass())}.
     */
    public RowOperations forRow(Object row) {
        return forClass(row == null ? null : row.getClass());
    }
}
