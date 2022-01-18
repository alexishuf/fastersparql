package com.github.alexishuf.fastersparql.client.parser.row;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.reactivestreams.Publisher;

import java.util.Collection;

/**
 * A factory for publishers that convert iterable {@link CharSequence}s or {@code byte[]}
 * into higher level representations of SELECT/ASK query solutions.
 *
 * Each {@link CharSequence} or {@code byte[]} fed represents a single and complete RDF term in
 * N-Triples syntax. Each iterable (array or Collection) represents a single solution (an
 * assignment of, possibly null, values to variables in the query). Thus, there is no need to
 * buffer {@link CharSequence}s or {@code byte[]}s to assemble a whole term or solution.
 *
 * @param <R> the resulting type after parsing. An intance of {@code R} may contain nulls or map
 *           to nulls, but the publisher shall never produce a null {@code R}.
 */
public interface RowParser<R> {
    /**
     * Get the {@link Class} object for the result Row type.
     *
     * @return A non-null {@link Class}
     */
    Class<? super R> rowClass();

    /**
     * Create a Publisher of one {@code R} for each array {@link CharSequence}, representing
     * a solution.
     *
     * {@code source} will never produce a {@code null} array but the array may contain nulls to
     * represent unbound variables in a solution.
     *
     * The returned {@link Publisher} must forward any errors notified by the {@code source}'s
     * {@link Results#publisher()} and is allowed to raise its own errors as well.
     *
     * @param source the source of solutions, as an array of RDF terms in N-Triples syntax.
     * @return a non-null {@link Publisher} producing {@code R} instances
     */
    Publisher<R> parseStringsArray(Results<? extends CharSequence[]> source);

    /**
     * Create a {@link Publisher} of one {@code R} instances per solution represented by a
     * {@link Collection} of {@link CharSequence}.
     *
     * {@code source} will never produce a null, but the {@link Collection}s may contain nulls
     * to indicate the variable at that position has no binding.
     *
     * The returned {@link Publisher} must forward any errors notified by the {@code source}'s
     * {@link Results#publisher()} and is allowed to raise its own errors as well.
     *
     * @param source the source of solutions.
     * @return a non-null {@link Publisher} of {@code R} instances
     */
    Publisher<R> parseStringsList(Results<? extends Collection<? extends CharSequence>> source);

    /**
     * Create a Publisher of one {@code R} for each array of {@code byte[]}, representing
     * a solution.
     *
     * {@code source} will never produce a {@code null} array but the array may contain nulls to
     * represent unbound variables in a solution.
     *
     * The returned {@link Publisher} must forward any errors notified by the {@code source}'s
     * {@link Results#publisher()} and is allowed to raise its own errors as well.
     *
     * @param source the source of solutions, as an array of RDF terms in N-Triples
     *               syntax encoded in UTF-8 as a {@code byte[]}.
     * @return a non-null {@link Publisher} producing {@code R} instances
     */
    Publisher<R> parseBytesArray(Results<byte[][]> source);

    /**
     * Create a {@link Publisher} of one {@code R} instances per solution represented by a
     * {@link Collection} of {@link CharSequence}.
     *
     * {@code source} will never produce a null, but the {@link Collection}s may contain nulls
     * to indicate the variable at that position has no binding.
     *
     * The returned {@link Publisher} must forward any errors notified by the {@code source}'s
     * {@link Results#publisher()} and is allowed to raise its own errors as well.
     *
     * @param source the source of solutions.
     * @return a non-null {@link Publisher} of {@code R} instances
     */
    Publisher<R> parseBytesList(Results<? extends Collection<byte[]>> source);
}
