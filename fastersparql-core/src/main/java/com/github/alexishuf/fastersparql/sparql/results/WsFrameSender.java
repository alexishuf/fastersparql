package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.model.rope.ByteSink;

/**
 * Object that {@link WsClientParserBIt} and {@link WsServerParserBIt} use to send WebSocket frames.
 */
public interface WsFrameSender<T extends ByteSink<T>> {
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
     * Get a {@link ByteSink} that can be given to {@link #sendFrame(ByteSink)} after being
     * filled with data.
     */
    T createSink();

    /**
     * If {@link #createSink()} was called but there will be no {@link #sendFrame(ByteSink)}
     * call, this method MUST be called to release resources associated with the created sink.
     *
     * @param sink A {@link ByteSink} created by {@link #createSink()}
     */
    void releaseSink(T sink);
}
