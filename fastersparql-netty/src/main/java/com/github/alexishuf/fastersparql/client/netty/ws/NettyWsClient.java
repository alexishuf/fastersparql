package com.github.alexishuf.fastersparql.client.netty.ws;

import io.netty.channel.ChannelHandlerContext;

public interface NettyWsClient extends AutoCloseable {
    /**
     * Open (or reuse an idle) WebSocket session and handle events with {@code handle}.
     *
     * Eventually one of the following methods will be called:
     * <ul>
     *     <li>{@link WsClientHandler#attach(ChannelHandlerContext, WsRecycler)}
     *         once a WebSocket session has been established</li>
     *     <li>{@link WsClientHandler#onError(Throwable)} with the reason for not establishing
     *         a WebSocket session (e.g., could not connect, WebSocket handshake failed, etc.).</li>
     * </ul>
     *
     * Note that after {@code handler.attach(ctx, recycler)}, {@code onError} will still be called
     * if an error occurrs after the session was established.
     *
     * @param handler listener for WebSocket session events.
     */
    void open(WsClientHandler handler);

    @Override void close();
}
