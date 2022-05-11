package com.github.alexishuf.fastersparql.client.netty.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;

/**
 * Handle {@link WebSocketFrame}s on a Websocket session.
 *
 */
public interface WsClientHandler {
    /**
     * Called once per instance when the handler is bound to a {@link Channel}.
     *
     * <strong>This method runs within the event loop. Do not block.</strong>
     *
     * @param ctx The {@link ChannelHandlerContext}
     * @param recycler A recycler to be called if this handler has no use for the WebSocket session
     *                 but the session is in a state that allows it to be reused by a new
     *                 {@link WsClientHandler} instance. Once {@link WsRecycler#recycle(Channel)}
     *                 is called, the effect is the same as if {@link WsClientHandler#detach()}
     *                 had been called (it will not, however) and no futher event will be notified
     *                 to this {@link WsClientHandler}.
     */
    void attach(ChannelHandlerContext ctx, WsRecycler recycler);

    /**
     * Called once per instance when the handler is detached from the channel:
     *
     * <ul>
     *     <li>{@link Channel} closes due to an error</li>
     *     <li>{@link Channel} closes due to a {@link Channel#close()} call</li>
     *     <li>{@link Channel} closes due to a close from the remote peer</li>
     *     <li>The WebSocket session (and Channel) has been recycled ({@link WsRecycler}.</li>
     * </ul>
     *
     * <strong>This method runs within the event loop. Do not block.</strong>
     */
    void detach();

    /**
     * Called when an exception is thrown from a {@link WsClientNettyHandler} method, from netty
     * or from a previous {@link ChannelHandler} in the pipeline of the bound {@link Channel}.
     *
     * <strong>This method runs within the event loop. Do not block.</strong>
     */
    void onError(Throwable cause);

    /**
     * Called for every {@link WebSocketFrame} received from the remote peer, in the same sequence
     * as frames are received.
     *
     * The {@link CloseWebSocketFrame} will be delivered to this method, but {@code ctx.close()}
     * will be called even if implementations ignore the {@link CloseWebSocketFrame}.
     *
     * <strong>This method runs within the event loop. Do not block.</strong>
     *
     * @param frame a {@link WebSocketFrame} object.
     */
    void onFrame(WebSocketFrame frame);
}
