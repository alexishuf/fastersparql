package com.github.alexishuf.fastersparql.client.parser.results;

/**
 * Stateful parser that receives adjacent fragments of a SPARQL results serialization and calls
 * methods from a {@link ResultsParserConsumer} attached during construction as results variables
 * and rows are parsed.
 */
public interface ResultsParser {
    /**
     * Feed the next input of the results serialization.
     *
     * Methods of the attached {@link ResultsParserConsumer} will be called from within this method
     * if the list of variables becomes known or if a new complete row is parsed.
     *
     * @param input a sequence of chars following the last fed sequence.
     * @throws IllegalStateException if {@link ResultsParser#end()} has been called.
     */
    void feed(CharSequence input);

    /**
     * Signal the end of input.
     */
    void end();
}
