package com.github.alexishuf.fastersparql.client.netty.ws;

import io.netty.channel.Channel;

@FunctionalInterface
public interface WsRecycler {
    /**
     * Submit an established WebSocket session for reuse.
     * <p>
     * The session must be in a state where the server is ready to receive any message that would
     * be sent on a just open session and no message in response to the previous
     * {@link WsClientHandler} would be sent by the server after this method is called.
     * <p>
     * Calling {@link Channel#close()} is a valid implementation for this method.
     *
     * @param channel the {@link Channel} to reuse.
     */
    void recycle(Channel channel);

    WsRecycler CLOSE = ch -> { if (ch.isOpen()) ch.close(); };
}
