package com.github.alexishuf.fastersparql.client.parser.results;

import com.github.alexishuf.fastersparql.client.util.MediaType;
import lombok.Getter;

public class NoParserException extends Exception {
    @Getter private final MediaType mediaType;

    public NoParserException(MediaType mediaType) {
        super("No parser for "+mediaType);
        this.mediaType = mediaType;
    }
}
