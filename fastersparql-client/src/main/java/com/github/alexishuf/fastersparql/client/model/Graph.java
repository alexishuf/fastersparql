package com.github.alexishuf.fastersparql.client.model;

import com.github.alexishuf.fastersparql.client.util.*;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A {@link Publisher} of RDF graph serialization fragments with the serialization media type.
 *
 * A serialization fragment is represented by Fragment, which may be an object representing an
 * RDF triple, a set of RDF triples or simply a fragment of the graph serialization.
 *
 * If {@code Fragment} is a {@link CharSequence} or {@link String}, consumers of the fragments
 * the concatenation of fragments will yield the serialization of the RDF graph. If
 * {@code Fragment} is an iterable of textual types, the same holds, but for the
 * concatenation of all {@link CharSequence} in all iterables.
 *
 * If {@code Fragment} is {@code byte[]} or an iterable of {@code byte[]}, the same
 * interpretation of textual types holds, with the bytes being the UTF-8 encoding of the
 * serialization, unless the serialization is a binary format, uses a default that is not UTF-8 or
 * includes a charset attribute in {@link Graph#mediaType()}.
 *
 * Consumers should not assume textual or bytes fragments end in the border of RDF terms,
 * triples or quads. An individual fragment may end in the middle of one RDF term and
 * may contain no complete term or triple/quad.
 *
 * @param <Fragment> - the type representing fragments of the RDF graph serialization.
 */
@Getter @Accessors(fluent = true)
@Slf4j
public class Graph<Fragment> {
    /**
     * The media type identifying the RDF graph serialization format.
     *
     * The task is safe: failures before or during parsing of the media type will not be reported
     * here and the {@link AsyncTask} will complete with media type {@code * /*}
     *
     * If an error occurs, it will be reported to {@link Subscriber#onError(Throwable)}
     * by {@link Graph#publisher()} and this method will return a null {@link MediaType}.
     */
    private final SafeAsyncTask<MediaType> mediaType;

    /**
     * The class of fragments produced by {@link Graph#publisher()}.
     */
    private final Class<? super Fragment> fragmentClass;

    /**
     * A non-null, single-subscription {@link Publisher} of RDF graph serialization fragments.
     *
     * If {@code Fragment} is {@code byte[]}, {@link CharSequence}, {@link String} or an iterable
     * of these, concatenating the byte or char sequences will yield the serialization of the RDF
     * graph. Consumers should not expect fragments to end on RDF term or triple/quad borders.
     */
    private final Publisher<Fragment> publisher;

    public Graph(SafeAsyncTask<MediaType> mediaType, Class<? super Fragment> fragmentClass,
                 Publisher<? extends Fragment> publisher) {
        this.mediaType = mediaType;
        this.fragmentClass = fragmentClass;
        //noinspection unchecked
        this.publisher = (Publisher<Fragment>) publisher;
    }

    /**
     * Get a {@link Future} for the charset specified in the media type, falling back to the given
     * {@link Charset} if the media type defines no charset parameter.
     *
     * The {@link Future} may throw these exceptions, wrapped in {@link ExecutionException}:
     * <ul>
     *     <li>{@link IllegalCharsetNameException} if the charset param has an invalid value</li>
     *     <li>{@link UnsupportedCharsetException} if the charset param is valid but the
     *         charset is not supported by the JVM</li>
     * </ul>
     *
     * @return a {@link Future} for the {@link Charset} set by {@link Graph#mediaType()}.
     */
    public AsyncTask<@PolyNull Charset> charset(@PolyNull Charset fallback) {
        CompletableAsyncTask<Charset> task = new CompletableAsyncTask<>();
        mediaType.whenComplete((mt, t) -> {
            try {
                task.complete(mt == null ? fallback : mt.charset(fallback));
            } catch (Throwable badCharsetThrowable) {
                task.completeExceptionally(badCharsetThrowable);
            }
        });
        return task;
    }

    /**
     * Calls {@link Graph#charset(Charset)} with UTF-8 as the fallback
     *
     * @return see {@link Graph#charset(Charset)}.
     */
    public AsyncTask<Charset> charset() { return charset(UTF_8); }

    /**
     * Wrap {@link Graph#publisher()} into a lazy {@link Iterable}.
     *
     * Since {@link Graph#publisher()} is single-subscription, calling {@link AsyncIterable#start()}
     * will disallow future calls to {@link Publisher#subscribe(Subscriber)} on
     * {@link Graph#publisher()}.
     *
     * @return a non-null {@link AsyncIterable} wrapping {@link Graph#publisher()}.
     */
    public AsyncIterable<Fragment> iterable() {
        return new IterableAdapter<>(publisher);
    }
}
