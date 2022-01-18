package com.github.alexishuf.fastersparql.client.parser.fragment;

import com.github.alexishuf.fastersparql.client.model.Graph;
import org.reactivestreams.Publisher;


/**
 * A factory for {@link Publisher}s that convert {@code byte[]} or {@link CharSequence}
 * fragments into a higher-level Fragment type.
 *
 * A typical example will be to feed the input bytes or text into an RDF parser and
 * produce a stream of triples or quads.
 *
 * @param <F> the resulting Fragment type.
 */
public interface FragmentParser<F> {
    /**
     * The {@link Class} of the produced fragment instances.
     * @return a non-null {@link Class};
     */
    Class<? super F> fragmentClass();

    /**
     * Create a {@link Publisher} that produces an R instance for one or more
     * {@link CharSequence}s it receives from {@code source}
     *
     * Neither {@code source} nor the returned {@link Publisher} ever produce a null value.
     * Note that the number of items produced by the returned {@link Publisher} is not linked
     * to the number of items produced by {@code source}: one {@link CharSequence} may yield
     * many {@code R} instances and many {@link CharSequence} instances may yield a single
     * {@code R} instance.
     *
     * The returned {@link Publisher} must forward errors notified by the {@code source}
     * {@link Graph#publisher()}.
     *
     * @param source the source of textual fragments of an RDF graph serialization.
     * @return A {@link Publisher} producing R instances from the concatenation of
     *         textual fragments.
     */
    Publisher<F> parseStrings(Graph<? extends CharSequence> source);

    /**
     * Create a {@link Publisher} that produces {@code R} instances from one or more {@code byte[]}
     * received from {@code source}.
     *
     * Neither {@code source} nor the returned Publisher ever produce nulls. Note that the number
     * of produces instances by the returned {@link Publisher} is not linked to the number of
     * instances produces by {@code source}: One {@code byte[]} may yield many {@code R} instances
     * and one {@code R} instance may need multiple {@code byte[]} instances to assemble.
     *
     * The returned {@link Publisher} must forward errors notified by the {@code source}
     * {@link Graph#publisher()}.
     *
     * @param source the source of byte sequences in a RDF graph serialization.
     * @return A {@link Publisher} that produces {@code R} instances from the byte sequences.
     */
    Publisher<F> parseBytes(Graph<byte[]> source);
}
