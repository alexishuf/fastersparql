package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.model.rope.ByteSink;

/**
 * Object that {@link WsClientParserBIt} and {@link WsServerParserBIt} use to send WebSocket frames.
 */
public interface WsFrameSender<S extends ByteSink<S, T>, T> {
    /**
     * Send the given content as a frame.
     *
     * <p>The given {@code content} instance should have been previously obtained from
     * {@link #createSink()}. Callers of this method MUST assume that ownership of
     * {@code content} is passed to this method and MUST not read/write to {@code content}
     * after this method returns</p>
     *
     * <p>An implementation may return from this method before confirming the frame has been
     * flushed all the way to the OS network stack. However, implementations are required to send
     * the frames in the same order as the corresponding calls to this method were made.</p>
     */
    void sendFrame(T content);

    /**
     * Get a {@link ByteSink} that can be given to {@link #sendFrame(Object)} after being
     * filled with data.
     */
    S createSink();

    /**
     * Create a {@link ResultsSender} that can serialize and send batches.
     *
     * <p>Using this instead of directly calling {@link #sendFrame(Object)} is preferable since
     * some implementations may have performance penalties if {@link #sendFrame(Object)} and
     * {@link #createSink()} are not called from an implementation specific thread.</p>
     */
    ResultsSender<S, T> createSender();
}
