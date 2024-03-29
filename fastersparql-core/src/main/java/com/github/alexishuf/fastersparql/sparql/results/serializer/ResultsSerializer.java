package com.github.alexishuf.fastersparql.sparql.results.serializer;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.model.ContentNegotiator;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.util.NamedService;
import com.github.alexishuf.fastersparql.util.NamedServiceLoader;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.nullness.qual.Nullable;

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

    public ResultsSerializer(MediaType contentType) {
        this.contentType = contentType;
    }

    protected void onInit() {}

    public final MediaType contentType() { return contentType; }

    /**
     * Write a results header with the given {@code subset} to {@code dest}.
     *
     * @param vars set of vars that name the columns in batches of subsequent calls to
     *             {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
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
     * A handler for heads detached from their linked list by
     * {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
     *
     * <p>To simply recycle nodes, use {@link #recycler(BatchType)}. To re-assembled the
     * linked list of serialized batches, use {@link Reassemble}.</p>
     *
     * @param <B> the class of the {@code batch} given to the {@code serialize()} call.;
     */
    public interface NodeConsumer<B extends Batch<B>> {
        /**
         * Called from {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
         * after all rows in {@code node} have been serialized into chunks and delivered to a
         * {@link ChunkConsumer}.
         *
         * <p>Ownership of {@code node} is transferred to this function, which should transfer
         * it elsewhere or recycle {@code node}. To simply recycle nodes, use
         * {@link #recycler(BatchType)}. To re-assembled the linked list of serialized batches,
         * use {@link Reassemble}.</p>
         *
         * @param node the node just serialized.
         * @throws Exception if something goes wrong. This exception will be rethrown as a
         *                   {@link SerializationException} from
         *                   {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
         */
        void onSerializedNode(B node) throws Exception;

        /**
         * Similar to {@link #onSerializedNode(Batch)}, but this method will be called if there
         * was a serialization error and {@code node} was not serialized
         * ({@code serializedUntilRow == 0}) or serialized only partially
         * ({@code serializedUntilRow > 0}).
         *
         * @param node the node that was not fully serialized
         * @param serializedUntilRow up until which row, {@code node} was serialized
         * @throws Exception Implementations can throw anything, which will be suppressed
         *                  (see {@link Throwable#addSuppressed(Throwable)}) since this is
         *                  called from within an exception handler.
         */
        default void onNotSerializedNode(B node, int serializedUntilRow) throws Exception {
            onSerializedNode(node);
        }
    }

    /**
     * A {@link NodeConsumer} that re-assembles the linked list of {@link Batch}es, as each
     * node gets serialized.
     *
     * <p><strong>Important: </strong> Whoever instantiates this, must eventually call
     * {@link #take()} to take ownership of the reassembled linked list.</p>
     */
    @MustCall("take")
    public static class Reassemble<B extends Batch<B>> implements NodeConsumer<B>, AutoCloseable {
        private @Nullable B head;

        /**
         * Take the linked list of {@link Batch}es reassembled by this instance.
         *
         * <p>This is not idempotent, a second call will return {@code null}, unless a new
         * {@link #onSerializedNode(Batch)} call arrives int the meantime.</p>
         * @return the reassembled linked list of {@link Batch}es.
         */
        public @Nullable B take() {
            B b = head;
            head = null;
            return b;
        }

        @Override public void close() { Batch.recycle(take()); }

        @Override public void onSerializedNode(B b) { head = Batch.quickAppend(head, b); }
    }

    /**
     * A {@link NodeConsumer} that reassembles the linked list of {@link Batch}es as each node
     * gets serialized by {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}.
     *
     * <p>Unlike {@link Reassemble}, this will not attempt to {@link Batch#recycle()} an
     * un-{@link Reassemble#take()}n batch </p>
     */
    public static class LeakingReassemble<B extends Batch<B>> implements NodeConsumer<B> {
        private @Nullable B head;
        @Override public void onSerializedNode(B b) { head = Batch.quickAppend(head, b); }
    }

    public record Recycler<B extends Batch<B>>(BatchType<B> t) implements NodeConsumer<B> {
        @Override public void    onSerializedNode(B b)              { t.recycle(b); }
    }

    /**
     * Get a {@link NodeConsumer} that {@link BatchType#recycle(Batch)}s the nodes.
     * @param type the {@link BatchType} of the batches to recycle
     * @return a lazy-init singleton {@link NodeConsumer} that calls {@code type.recycle(b)}
     */
    public static <B extends Batch<B>> Recycler<B> recycler(BatchType<B> type) {
        //noinspection unchecked
        Recycler<B> r = (Recycler<B>)RECYCLERS[type.id];
        if (r == null)
            RECYCLERS[type.id] = r = new Recycler<>(type);
        return r;
    }
    private static final Recycler<?>[] RECYCLERS = new Recycler[BatchType.MAX_BATCH_TYPE_ID];

    public static final class SerializationException extends RuntimeException {
        public SerializationException(String message) {super(message);}
        public SerializationException(String message, Throwable cause) {super(message, cause);}
    }

    /**
     * Consume a chunk produced by
     * {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
     */
    public interface ChunkConsumer<T> {
        /**
         * Do something useful with the given {@code chunk}. Most implementations will want to
         * write the chunk somewhere
         *
         * <p>This call is synchronous: no further serialization (converting rows of a
         * {@link Batch} into bytes inside a chunk) will occur while this method executes.
         * After its return, serialization will resume using the same {@link ByteSink} that
         * yielded the {@code chunk} given in this call.</p>
         *
         * <p>Ownership of {@code chunk} is transferred to this method, if the {@code chunk}
         * class has manual lifecycle (e.g., netty {@code ByteBuf}). Therefore, implementations
         * must forward ownership elsewhere or release the {@code chunk}.</p>
         *
         * @param chunk a chunk of serialized {@link Batch} data.
         * @throws Exception if something goes wrong, such as I/O error. The exception will be
         *                   re-thrown as a {@link SerializationException} from
         *                   {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}.
         */
        void onSerializedChunk(T chunk) throws Exception;

        /**
         * If this method returns {@code true} at
         * {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)} entry,
         * {@link ByteSink#take()} and {@link #onSerializedChunk(Object)} will not be called.
         * Note that if {@code hardMaxBytes} is not large enough,
         * {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)} will fail
         * when {@link ByteSink#len()} exceeds it.
         */
        default boolean isNoOp() {return false;  }
    }

    private static final ChunkConsumer<?> IGNORE_CHUNKS = new ChunkConsumer<>() {
        @Override public void onSerializedChunk(Object ignored) {}
        @Override public boolean isNoOp() {return true;}
    };

    /**
     * Get a singleton {@link ChunkConsumer} that does nothing. Useful with {@link ByteSink}
     * implementations that write directly to a non-readable destination, such as a
     * {@link java.io.OutputStream}. Such {@link ByteSink#take()} implementations are dummies
     * and thus should be consumed by a dummy.
     */
    public static <T> ChunkConsumer<T> ignoreChunks() { //noinspection unchecked
        return (ChunkConsumer<T>)IGNORE_CHUNKS;
    }


    /**
     * Destructively serialize {@code batch} and subsequent nodes using {@code sink} to
     * build chunks which have at most {@code hardMaxBytes} bytes, but ideally no more than
     * {@code softMaxBytes}.
     *
     * <p>Destructively means that after the contents of a node have been serialized into
     * {@code sink}, the node is detached from the remainder of the linked list using
     * {@link Batch#detachHead()} and given synchronously to {@code nodeConsumer}. </p>
     *
     * <p>rows are serialized as bytes into the given {@code sink}, until all rows have been
     * serialized or {@code sink.len()} is near or above {@code softMaxBytes}. Implementations
     * of this method should create the biggest possible chunks that are still smaller than
     * {@code softMaxBytes}. However, if a single row serialization requires more than
     * {@code softMaxBytes} bytes, that rule can be violated, so long as the chunk still remains
     * below {@code hardMaxBytes}. If {@code hardMaxBytes} is exceeded, the chunk is not
     * delivered and a {@link SerializationException} is thrown.</p>
     *
     * <p>Serialized chunks are obtained from {@code sink.take()} and delivered synchronously
     * to the {@code chunkConsumer}. If the {@code chunk} class has manual lifecycle management,
     * {@code nodeConsumer} receives ownership of the chunk, and must forward it or release
     * the chunk.</p>
     *
     * @param batch         the head of the linked list of {@link Batch}es to be serialized
     * @param sink          the {@link ByteSink} that will be used to repeatedly build chunks
     *                      of serialized rows
     * @param hardMaxBytes  Fail serialization if a single row requires this many bytes or more
     * @param nodeConsumer  object that will receive nodes after their contents have been
     *                      serialized and the nodes have been detached from the subsequent nodes
     * @param chunkConsumer object that will receive each chunk, as it is created.
     */
    public abstract <B extends Batch<B>, S extends ByteSink<S, T>, T>
    void serialize(Batch<B> batch, ByteSink<S, T> sink, int hardMaxBytes,
                   NodeConsumer<B> nodeConsumer,
                   ChunkConsumer<T> chunkConsumer);

    /**
     * Equivalent to {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
     * with:
     *
     * <ul>
     *     <li>{@code softMaxBytes =} {@link Integer#MAX_VALUE}</li>
     *     <li>{@code hardMaxBytes =} {@link Integer#MAX_VALUE}</li>
     *     <li>{@code nodeConsumer = new} {@link LeakingReassemble}</li>
     *     <li>{@code chunkConsumer = } {@link #ignoreChunks()}</li>
     * </ul>
     *
     * @param batch the batch to serialize
     * @param sink destination of serialization bytes
     */
    public <B extends Batch<B>, S extends ByteSink<S, T>, T>
    void serialize(Batch<B> batch, ByteSink<S, T> sink) {
        serialize(batch, sink, Integer.MAX_VALUE,
                  new Reassemble<>(), ignoreChunks());
    }

    /**
     * Serialize a single row from {@code batch} into {@code sink}.
     *
     * @param batch a {@link Batch} with {@link Batch#rows()} {@code > row}
     * @param sink destination of the serialization of the row
     * @param row index  {@code >= 0} and {@code < batch.rows} of a row in {@code batch}
     */
    public abstract
    void serialize(Batch<?> batch, ByteSink<?, ?> sink, int row);

    /** Helper for use by {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
     *  implementations */
    protected <S extends ByteSink<S, T>, T>
    void deliver(ByteSink<S, T> sink, ChunkConsumer<T> chunkConsumer, int rowsInChunk,
                    int lastSinkLen, int maxBytes) {
        int len = sink.len();
        if (rowsInChunk > 1 && len >= maxBytes)
            len = lastSinkLen;
        if (len > maxBytes)
            throw new SerializationException("maxBytes exceeded");
        T chunk = sink.takeUntil(len);
        try {
            chunkConsumer.onSerializedChunk(chunk);
        } catch (Throwable e) {
            throw new SerializationException("chunkConsumer failed", e);
        }
    }

    /** Helper for use by {@link #serialize(Batch, ByteSink, int, NodeConsumer, ChunkConsumer)}
     *  implementations */
    protected static <B extends Batch<B>>
    @Nullable B detachAndDeliverNode(B batch, NodeConsumer<B> nodeConsumer) {
        B next;
        next = batch.detachHead();
        try {
            nodeConsumer.onSerializedNode(batch);
        } catch (Throwable e) {
            throw new SerializationException("nodeConsumer failure", e);
        }
        return next;
    }

    protected static <B extends Batch<B>>
    void handleNotSerialized(B batch, int serializedUntilRow, NodeConsumer<B> consumer,
                             Throwable error) {
        if (batch == null) return;
        B remainder = batch.detachHead();
        try {
            consumer.onNotSerializedNode(batch, serializedUntilRow);
        } catch (Exception e) {
            error.addSuppressed(e);
        }
        for (batch = remainder; remainder != null; batch = remainder) {
            remainder = batch.detachHead();
            try {
                consumer.onNotSerializedNode(batch, 0);
            } catch (Exception e) {
                error.addSuppressed(e);
            }
        }
    }

    /** Write to {@code dest} any required UTF-8 bytes to complete the serialization */
    public abstract void serializeTrailer(ByteSink<?, ?> dest);
}
