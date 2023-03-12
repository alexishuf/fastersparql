package com.github.alexishuf.fastersparql.sparql.results;

import com.github.alexishuf.fastersparql.model.rope.Rope;

/**
 * Object that {@link WsClientParserBIt} and {@link WsServerParserBIt} use to send WebSocket frames.
 */
@FunctionalInterface
public interface WsFrameSender {

    /**
     * Send the given rope as a UTF-8 text WebSocket frame to the remote peer.
     *
     * <p>An implementation may return from this method before confirming the frame has been
     * flushed all the way to the OS network stack. However, implementations are required to send
     * the frames in the same order as the corresponding calls to this method were made.</p>
     */
    void sendFrame(Rope content);
}
