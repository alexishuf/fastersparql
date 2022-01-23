package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ResultsParserRegistry {
    private static final ResultsParserRegistry INSTANCE;

    static {
        ResultsParserRegistry factory = new ResultsParserRegistry();
        factory.register(SparqlResultFormat.TSV.asMediaType(), TSVParser::new);
        factory.register(SparqlResultFormat.JSON.asMediaType(), JsonParser::new);
        factory.register(SparqlResultFormat.CSV.asMediaType(), CSVParser::new);
        INSTANCE = factory;
    }

    private final Map<MediaType, Function<ResultsParserConsumer, ResultsParser>> mt2Factory
            = new HashMap<>();

    public static ResultsParserRegistry get() {
        return INSTANCE;
    }

    public synchronized void register(MediaType mediaType,
                                      Function<ResultsParserConsumer, ResultsParser> factory) {
        mt2Factory.put(mediaType, factory);
    }

    /**
     * Tests if {@link ResultsParserRegistry#createFor(MediaType, ResultsParserConsumer)} would
     * throw a {@link NoParserException}.
     *
     * @param mediaType the media type of the SPARQL results.
     * @return {@code true} iff a {@link ResultsParser} for {@code mediaType} can be created.
     */
    public boolean canParse(MediaType mediaType) {
        return mt2Factory.containsKey(mediaType.withoutParams());
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
        Function<ResultsParserConsumer, ResultsParser> f;
        f = mt2Factory.getOrDefault(mediaType.withoutParams(), null);
        if (f == null)
            throw new NoParserException(mediaType);
        return f.apply(consumer);
    }
}
