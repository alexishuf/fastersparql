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
    protected final SparqlResultFormat format;
    protected final String contentType;
    protected boolean ask = false, empty = true;
    protected Vars vars = Vars.EMPTY;

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

    public ResultsSerializer(SparqlResultFormat format, String contentType) {
        this.format = format;
        this.contentType = contentType;
    }

    protected abstract void init(Vars subset, ByteSink<?, ?> dest);

    public final SparqlResultFormat format() { return format; }
    public final String contentType() { return contentType; }

    /**
     * Write a results header with the given {@code subset} to {@code dest}.
     *
     * @param vars set of vars that name the columns in batches of subsequent calls to
     *             {@link #serialize(Batch, int, int, ByteSink)}
     * @param subset subset of {@code vars} that names the columns that shall be serialized
     * @param ask whether to serialize an results for an ASK query
     * @param dest where the results header will be appended to
     */
    public final void init(Vars vars, Vars subset, boolean ask, ByteSink<?, ?> dest) {
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
        init(subset, dest);
    }

    /** Equivalent to {@link #serialize(Batch, int, int, ByteSink)} spanning the whole batch */
    public void serialize(Batch<?> batch, ByteSink<?, ?> dest) {
        serialize(batch, 0, batch == null ? 0 : batch.rows, dest);
    }

    /** Serialize rows {@code [begin,begin+nRows)} from {@code batch} to {@code dest}. */
    public abstract void serialize(Batch<?> batch, int begin, int nRows, ByteSink<?, ?> dest);

    /** Write to {@code dest} any required UTF-8 bytes to complete the serialization */
    public abstract void serializeTrailer(ByteSink<?, ?> dest);
}
