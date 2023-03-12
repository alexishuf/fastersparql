package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.LazyBIt;
import com.github.alexishuf.fastersparql.model.MediaType;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A bundle of a {@link BIt} over graph fragments and the {@link MediaType} of the
 * graph representation.
 *
 * <p>Consumers should not assume textual or bytes fragments end in the border of RDF terms,
 * triples or quads. An individual fragment may end in the middle of one RDF term and
 * may contain no complete term or triple/quad.</p>
 *
 */
public final class Graph{
    private final Future<MediaType> mediaTypeFuture;
    public final BIt<byte[]> it;

    public Graph(Future<MediaType> mediaTypeFuture, BIt<byte[]> it) {
        this.mediaTypeFuture = mediaTypeFuture;
        this.it = it;
    }

    public BIt<byte[]> it() { return it; }

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