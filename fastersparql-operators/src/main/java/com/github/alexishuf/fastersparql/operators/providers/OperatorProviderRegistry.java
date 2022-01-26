package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.operators.NoOperatorProviderException;
import com.github.alexishuf.fastersparql.operators.Operator;
import com.github.alexishuf.fastersparql.operators.OperatorName;

import java.util.*;
import java.util.function.Function;

public class OperatorProviderRegistry {
    private final Map<OperatorName, Set<OperatorProvider>> providerMap = new HashMap<>();

    /**
     * Scan the classpath for implementations of all the {@link OperatorProvider} sub-interfaces
     * using {@link ServiceLoader} and register the found {@link OperatorProvider}s.
     *
     * This method is not thread-safe.
     *
     * @return {@code this} registry
     */
    public OperatorProviderRegistry registerAll() {
        for (OperatorName name : OperatorName.values()) {
            for (OperatorProvider provider : ServiceLoader.load(name.providerClass())) {
                register(provider);
            }
        }
        return this;
    }

    private static final Function<OperatorName, Set<OperatorProvider>> SET_FACTORY
            = k -> new HashSet<>();

    /**
     * Register an {@link OperatorProvider}.
     *
     * This method is not thread-safe
     *
     * @param provider the {@link OperatorProvider} instance to register.
     */
    public void register(OperatorProvider provider) {
        providerMap.computeIfAbsent(provider.operatorName(), SET_FACTORY).add(provider);
    }

    /**
     * Creates a new {@link Operator} of the given type {@code T} with the given {@code flags}.
     *
     * This will scan all registered {@link OperatorProvider}s able to provide a {@code T}
     * instance and use the provider with lowest {@link OperatorProvider#bid(long)} to create a
     * new instance.
     *
     * @param cls The {@link Class} instance for {@code T}
     * @param flags the flags that will be passed to the candidates and to the selected
     *              {@link OperatorProvider}.
     * @param <T> the {@link Operator} sub-interface.
     * @return a new {@link Operator} of type {@code T}
     * @throws NoOperatorProviderException if there is no {@link OperatorProvider} with a bid
     *         below {@link Integer#MAX_VALUE}
     */
    public <T extends OperatorProvider> T get(Class<T> cls, long flags)
            throws NoOperatorProviderException {
        //noinspection unchecked
        return (T)get(OperatorName.valueOfProvider(cls), flags);
    }


    /**
     * Version of {@link OperatorProviderRegistry#get(Class, long)} that takes an
     * {@link OperatorName} instead of a {@link Class}.
     */
    public OperatorProvider get(OperatorName name, long flags) throws NoOperatorProviderException {
        OperatorProvider best = null;
        int bestBid = Integer.MAX_VALUE;
        int rejected = 0;
        for (OperatorProvider candidate : providerMap.getOrDefault(name, Collections.emptySet())) {
            int bid = candidate.bid(flags);
            if (bid < bestBid) {
                bestBid = bid;
                best = candidate;
            } else if (bid == Integer.MAX_VALUE) {
                ++rejected;
            }
        }
        if (best == null) {
            if (rejected == 0)
                throw NoOperatorProviderException.noProviders(name.asClass(), flags);
            throw NoOperatorProviderException.rejectingProviders(name.asClass(), flags);
        } else {
            return best;
        }
    }
}
