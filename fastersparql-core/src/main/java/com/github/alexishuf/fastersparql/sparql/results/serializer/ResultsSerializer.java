package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.ContentNegotiator;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;

import java.util.Map;

public abstract class ResultsSerializer {
    protected int[] columns = null;
    protected final MediaType contentType;
    protected boolean ask = false, empty = true;
    protected Vars vars = Vars.EMPTY, subset = Vars.EMPTY;

    public interface Factory extends NamedService<SparqlResultFormat> {
        ResultsSerializer create(Map<String, String> params);
        default MediaType offers() { return name().asMediaType(); }
    }

    public static final class NoSerializerException extends FSException {
        public NoSerializerException(String message) { super(message); }
        public NoSerializerException(SparqlResultFormat fmt) { this("No serializer for "+fmt); }
    }

    private static final NamedServiceLoader<Factory, SparqlResultFormat> NSL
            = new NamedServiceLoader<>(Factory.class) {
        @Override protected Factory fallback(SparqlResultFormat name) {
            throw new NoSerializerException(name);
        }
    };

    /**
     * Creates a {@link ContentNegotiator} for matching {@code Accept} headers against the
     * serializers available.
     *
     * @return a new {@link ContentNegotiator}
     */
    public static ContentNegotiator contentNegotiator() {
        var names = NSL.names();
        MediaType[] offers = new MediaType[names.size()];
        int i = 0;
        for (SparqlResultFormat name : names)
            offers[i++] = NSL.get(name).offers();
        return new ContentNegotiator(offers);
    }

    /** Whether there is a {@link ResultsSerializer} implementation that outputs {@code fmt}. */
    public static boolean supports(SparqlResultFormat fmt) { return NSL.has(fmt); }

    /**
     * Create a {@link ResultsSerializer} that writes in the given {@code fmt}.
     *
     * @return a new {@link ResultsSerializer}
     * @throws NoSerializerException if there is no serializer that supports {@code fmt}.
     */
    public static ResultsSerializer create(SparqlResultFormat fmt) {
        return NSL.get(fmt).create(Map.of());
    }

    /**
     * Creates a {@link ResultsSerializer} for the {@link SparqlResultFormat} corresponding to
     * {@code mediaType} with format-specific parameters (e.g., {@code charset}) in
     * {@link MediaType#params()}.
     *
     * @return a new {@link ResultsSerializer}
     * @throws NoSerializerException if there is no serializer that supports the media type.
     */
    public static ResultsSerializer create(MediaType mediaType) {
        return NSL.get(SparqlResultFormat.fromMediaType(mediaType)).create(mediaType.params());
    }

    public ResultsSerializer(String contentType) {
        this.contentType = contentType;
    }

    protected void onInit() {}

    public final String contentType() { return contentType; }

    /**
     * Write a results header with the given {@code subset} to {@code dest}.
     *
     * @param vars set of vars that name the columns in batches of subsequent calls to
     *             {@link #serialize(Batch, int, int, ByteSink)}
     * @param subset subset of {@code vars} that names the columns that shall be serialized
     * @param ask whether to serialize an results for an ASK query
     */
    public final void init(Vars vars, Vars subset, boolean ask) {
        int[] columns = this.columns;
        if (columns == null || columns.length != subset.size())
            this.columns = columns = new int[subset.size()];
        if (subset == vars) {
            for (int i = 0; i < columns.length; i++)  columns[i] = i;
        } else {
            for (int i = 0; i < columns.length; i++) {
                if ((columns[i] = vars.indexOf(subset.get(i))) < 0)
                    throw new IllegalArgumentException("subset is not a subset of vars");
            }
        }
        this.ask = ask;
        this.empty = true;
        this.vars = vars;
        this.subset = subset;
        onInit();
    }

    /**
     * Writes a results header to {@code dest} using the vars previously set via {@link #init(Vars, Vars, boolean)}.
     *
     * @param dest destination of the header serialization
     */
    public abstract void serializeHeader(ByteSink<?, ?> dest);

    /**
     * {@link #serialize(Batch, int, int, ByteSink)} {@code batch} and all its successor nodes
     * ({@link Batch#next()})
     */
    public void serializeAll(Batch<?> batch, ByteSink<?, ?> dest) {
        for (var b = batch; b != null; b = b.next)
            serialize(b, 0, b.rows, dest);
    }

    /**
     * {@link #serializeAll(Batch, ByteSink)}, but executes {@code nodeConsumer.accept(b)}
     * for every batch {@code b} in the linked list rooted at {@code batch}.
     *
     * @param batch the head of a linked list of batches
     * @param dest where the serialization of {@code batch} will be written to
     * @param nodeConsumer callback for every individual batch in the
     *                     linked list that starts with {@code batch}
     */
    public <B extends Batch<B>> void serializeAll(B batch, ByteSink<?, ?> dest,
                                                  SerializedNodeConsumer<B> nodeConsumer) {
        for (B b = batch, n; b != null; b = n) {
            serialize(b, 0, b.rows, dest);
            n = b.detachHead();
            nodeConsumer.onSerializedNode(b);
        }
    }

    public interface SerializedNodeConsumer<B extends Batch<B>> {
        void onSerializedNode(B node);
    }

    /** Serialize rows {@code [begin,begin+nRows)} from {@code batch} to {@code dest}. */
    public abstract void serialize(Batch<?> batch, int begin, int nRows, ByteSink<?, ?> dest);

    /** Write to {@code dest} any required UTF-8 bytes to complete the serialization */
    public abstract void serializeTrailer(ByteSink<?, ?> dest);
}
