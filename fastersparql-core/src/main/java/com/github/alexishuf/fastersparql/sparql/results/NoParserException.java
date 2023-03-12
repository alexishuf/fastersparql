package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;

public class NoParserException extends FSException {
    private final SparqlResultFormat format;

    public NoParserException(SparqlResultFormat format) {
        super("No parser for "+format);
        this.format = format;
    }

    @SuppressWarnings("unused") public SparqlResultFormat format() { return format; }
}
