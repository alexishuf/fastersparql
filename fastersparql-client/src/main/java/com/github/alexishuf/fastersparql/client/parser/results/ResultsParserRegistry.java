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

    public ResultsParser createFor(MediaType mediaType,
                                   ResultsParserConsumer consumer) throws  NoParserException {
        Function<ResultsParserConsumer, ResultsParser> f;
        f = mt2Factory.getOrDefault(mediaType.withoutParams(), null);
        if (f == null)
            throw new NoParserException(mediaType);
        return f.apply(consumer);
    }
}
