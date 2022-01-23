package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

public class ResultsParserRegistry {
    private static final ResultsParserRegistry INSTANCE = new ResultsParserRegistry().registerAll();

    private final Map<MediaType, ResultsParserProvider> mt2Provider = new HashMap<>();

    /**
     * Get a global {@link ResultsParserProvider} already initialized with
     * {@link ResultsParserRegistry#registerAll()}
     *
     * @return a non-null shared {@link ResultsParserRegistry}.
     */
    public static ResultsParserRegistry get() {
        return INSTANCE;
    }

    /**
     * Register a specific {@link ResultsParserProvider}.
     *
     * Any previous {@link ResultsParserProvider} registered to one of
     * {@code provider.mediaTypes()} will be replaced with {@code provider.}
     *
     * @param provider the provider to register
     */
    public synchronized void register(ResultsParserProvider provider) {
        for (MediaType mt : provider.mediaTypes())
            mt2Provider.put(mt, provider);
    }

    /**
     * Uses a {@link ServiceLoader} to register all {@link ResultsParserProvider} implementations.
     *
     * @return {@code this} {@link ResultsParserRegistry}
     */
    public ResultsParserRegistry registerAll() {
        for (ResultsParserProvider provider : ServiceLoader.load(ResultsParserProvider.class)) {
            register(provider);
        }
        return this;
    }

    /**
     * Tests if {@link ResultsParserRegistry#createFor(MediaType, ResultsParserConsumer)} would
     * throw a {@link NoParserException}.
     *
     * @param mediaType the media type of the SPARQL results.
     * @return {@code true} iff a {@link ResultsParser} for {@code mediaType} can be created.
     */
    public boolean canParse(MediaType mediaType) {
        return mt2Provider.containsKey(mediaType.withoutParams());
    }

    /**
     * Creates a new {@link ResultsParser} for the given {@code mediaType}.
     *
     * Each {@link ResultsParser} shall be used to parse a single SPARQL results stream,
     * feeding events to a single {@link ResultsParserConsumer}.
     *
     * @param mediaType The {@link MediaType} of the results format to be parsed.
     * @param consumer A {@link ResultsParserConsumer} to receive events from the parser.
     * @return a new {@link ResultsParser} bound to {@code consumer}
     * @throws NoParserException if there is no parser implementation for the given media type.
     */
    public ResultsParser createFor(MediaType mediaType,
                                   ResultsParserConsumer consumer) throws  NoParserException {
        ResultsParserProvider provider;
        provider = mt2Provider.getOrDefault(mediaType.withoutParams(), null);
        if (provider == null)
            throw new NoParserException(mediaType);
        return provider.create(consumer);
    }
}
