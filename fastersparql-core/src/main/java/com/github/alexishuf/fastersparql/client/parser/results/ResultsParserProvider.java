package com.github.alexishuf.fastersparql.client.parser.results;


import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.List;

public interface ResultsParserProvider {
    List<MediaType> mediaTypes();
    ResultsParser create(ResultsParserConsumer consumer);
}
