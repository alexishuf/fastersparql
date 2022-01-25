package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.operators.providers.OperatorProvider;
import com.github.alexishuf.fastersparql.operators.providers.OperatorProviderRegistry;

public class FasterSparqlOps {
    private static final OperatorProviderRegistry registry
            = new OperatorProviderRegistry().registerAll();

    /**
     * Create an instance of the given {@link Operator} interface that best matches the given flags.
     *
     * Creation of an {@link Operator} instance is done by the {@link OperatorProvider}
     * corresponding to the {@link Operator} sub-interface given by {@code cls}. If there is more
     * than one suitable provider the one that produces the lowest
     * {@link OperatorProvider#bid(long)} for the given {@code flags} will be selected.
     *
     * @param cls An interface extending {@link Operator} denoting the {@link Operator} type to
     *            be instantiated.
     * @param flags A bitset of flags which provide information about the operands to be passed,
     *             about desired characteristics of the {@link Operator} implementation and
     *              about permissions for the implementation to deviate from standard SPARQL
     *              semantics. See {@link OperatorFlags} for a list of built-in flags.
     * @throws NoOperatorProviderException if there is no {@link OperatorProvider} for the given
     *         {@code cls} that returns a {@link OperatorProvider#bid(long)} for {@code flags}
     *         below {@link Integer#MAX_VALUE}.
     */
    public static <T extends Operator> T
    create(Class<T> cls, long flags) throws NoOperatorProviderException {
        return registry.create(cls, flags);
    }

    /**
     * Equivalent to {@link FasterSparqlOps#create(Class, long)} with {@code name.asClass()}.
     */
    public Operator create(OperatorName name, long flags) throws NoOperatorProviderException {
        return registry.create(name, flags);
    }
}
