package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.LazyBIt;
import com.github.alexishuf.fastersparql.client.util.MediaType;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A bundle of a {@link BIt} over graph fragments and the {@link MediaType} of the
 * graph representation.
 *
 * <p>If {@code Fragment} is a {@link CharSequence} or {@link String}, consumers of the fragments
 * the concatenation of fragments will yield the serialization of the RDF graph. If
 * {@code Fragment} is an iterable of textual types, the same holds, but for the
 * concatenation of all {@link CharSequence} in all iterables.</p>
 *
 * <p>If {@code Fragment} is {@code byte[]} or an iterable of {@code byte[]}, the same
 * interpretation of textual types holds, with the bytes being the UTF-8 encoding of the
 * serialization, unless the serialization is a binary format, uses a default that is not UTF-8 or
 * includes a charset attribute in {@link Graph#mediaType()}.</p>
 *
 * <p>Consumers should not assume textual or bytes fragments end in the border of RDF terms,
 * triples or quads. An individual fragment may end in the middle of one RDF term and
 * may contain no complete term or triple/quad.</p>
 *
 * @param <Fragment> - the type representing fragments of the RDF graph serialization.
 */
public final class Graph<Fragment>{
    private final Future<MediaType> mediaTypeFuture;
    public final BIt<Fragment> it;

    public Graph(Future<MediaType> mediaTypeFuture, BIt<Fragment> it) {
        this.mediaTypeFuture = mediaTypeFuture;
        this.it = it;
    }

    public BIt<Fragment> it() { return it; }

    /**
     * The media type identifying the RDF graph serialization format. This method may block
     * if the server has not yet sent the media type
     */
    public MediaType mediaType() {
        boolean interrupted = false;
        if (it instanceof LazyBIt<?> lazy)
            lazy.start();
        while (true) {
            try {
                if (interrupted)
                    Thread.currentThread().interrupt();
                return mediaTypeFuture.get();
            } catch (InterruptedException e) {
                interrupted = true;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException c) throw c;
                else if (cause instanceof Error c) throw c;
                else throw new RuntimeException(cause);
            }
        }
    }
}