package com.github.alexishuf.fastersparql.client.parser.results;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public interface ResultsParserConsumer {
    /**
     * Called only once, as soon as the list of variables declared in the results is known.
     *
     * @param vars the list of variables, without the leading {@code ?}s
     */
    void vars(List<String> vars);

    /**
     * Called once for each row parsed from the input.
     *
     * @param row an array of N-Triples terms. The i-th term is a binding to the
     *            i-th variable given to {@link ResultsParserConsumer#vars(List)}. Unbound
     *            variables appear as nulls
     */
    void row(@Nullable String[] row);

    /**
     * Signals the end of parseable input. Possible reasons:
     *
     * <ul>
     *     <li>The parser determined the results serialization has ended. There may be
     *     additional input, but it is not part of the serialization and should be ignored</li>
     *     <li>The input has ended and the parser is in a state where that is not an error.</li>
     *     <li>An error has occurred ({@link ResultsParserConsumer#onError(String)} would've
     *     been already called at this point</li>
     * </ul>
     */
    void end();

    /**
     * The input is invalid. After this method returns, {@link ResultsParserConsumer#end()}
     * will also be called.
     *
     * @param message A message describing the error in the input.
     */
    void onError(String message);
}
