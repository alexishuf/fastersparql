package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;
import com.github.alexishuf.fastersparql.client.util.MediaType;

public class NoParserException extends SparqlClientException {
    private final MediaType mediaType;

    public NoParserException(MediaType mediaType) {
        super("No parser for "+mediaType);
        this.mediaType = mediaType;
    }

    @SuppressWarnings("unused") public MediaType mediaType() { return mediaType; }
}
