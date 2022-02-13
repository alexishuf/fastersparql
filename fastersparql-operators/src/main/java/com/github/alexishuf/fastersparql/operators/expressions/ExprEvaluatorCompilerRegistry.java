package com.github.alexishuf.fastersparql.operators.expressions;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ExprEvaluatorCompilerRegistry {
    private static final ExprEvaluatorCompilerRegistry INSTANCE
            = new ExprEvaluatorCompilerRegistry();
    private @Nullable ExprEvaluatorCompilerProvider preferred;
    private final Map<String, ExprEvaluatorCompilerProvider> name2provider
            = new ConcurrentHashMap<>();

    public static ExprEvaluatorCompilerRegistry get() {
        return INSTANCE;
    }

    public void register(ExprEvaluatorCompilerProvider provider) {
        name2provider.put(provider.name().trim().toLowerCase(), provider);
        if (preferred == null || provider.order() < preferred.order())
            preferred = provider;
    }

    public void registerAll() {
        ServiceLoader.load(ExprEvaluatorCompilerProvider.class).forEach(this::register);
    }

    public List<ExprEvaluatorCompilerProvider> allProviders() {
        if (name2provider.isEmpty())
            registerAll();
        return new ArrayList<>(name2provider.values());
    }

    /**
     * Get an {@link ExprEvaluatorCompiler} from a provider with the given name or from the
     * preferred provider (by {@link ExprEvaluatorCompilerProvider#order()}) if there is no
     * provider with the given name.
     *
     * @param orName the desired provider name, or null. Comparison is case-insensitive.
     * @return a non-null {@link ExprEvaluatorCompiler}
     * @throws NoSuchElementException if there is no registered provider and no provider
     *                                can be found via SPI.
     */
    public ExprEvaluatorCompiler preferred(@Nullable String orName) {
        if (name2provider.isEmpty())
            registerAll();
        orName = orName == null ? "" : orName.trim().toLowerCase();
        ExprEvaluatorCompilerProvider provider = name2provider.getOrDefault(orName, null);
        if (provider == null)
            provider = preferred;
        if (provider == null)
            throw new NoSuchElementException("No providers found!");
        return provider.get();
    }

    /**
     * Gets a {@link ExprEvaluatorCompiler} created by a provider with the given name.
     *
     * @param name the name of the desired provider. Comparison will be case-insensitve.
     * @return the {@link ExprEvaluatorCompiler} or {@code null} if there is no provider
     *         with given name
     */
    public @Nullable ExprEvaluatorCompiler byName(String name) {
        if (name2provider.isEmpty())
            registerAll();
        ExprEvaluatorCompilerProvider provider;
        provider = name2provider.getOrDefault(name.trim().toLowerCase(), null);
        return provider == null ? null : provider.get();
    }
}
