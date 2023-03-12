package com.github.alexishuf.fastersparql.client.netty.ws;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Handle {@link WebSocketFrame}s on a Websocket session.
 *
 */
public interface NettyWsClientHandler {
    /**
     * Called once per instance when the handler is bound to a {@link Channel}.
     * <p>
     * <strong>This method runs within the event loop. Do not block.</strong>
     *
     * @param ctx The {@link ChannelHandlerContext}
     * @param recycler If the {@link NettyWsClientHandler} implementation is done with the
     *                 Websocket, but the session is in a usable state by a new
     *                 {@link NettyWsClientHandler}, it should call {@code recycler.release()},
     *                 else the {@link Channel} (or {@link ChannelHandlerContext}) should be
     *                 {@code close()}ed
     */
    void attach(ChannelHandlerContext ctx, ChannelRecycler recycler);

    /**
     * Called once per instance when the handler is detached from the channel:
     *
     * <ul>
     *     <li>{@link Channel} closes due to an error</li>
     *     <li>{@link Channel} closes due to a {@link Channel#close()} call</li>
     *     <li>{@link Channel} closes due to a close from the remote peer</li>
     *     <li>Could not acquire a {@link Channel} to the remote peer (this will be called without a previous attach())</li>
     * </ul>
     *
     * <strong>This method runs within the event loop. Do not block.</strong>
     */
    void detach(@Nullable Throwable error);

    /**
     * Called for every {@link WebSocketFrame} received from the remote peer, in the same sequence
     * as frames are received.
     * <p>
     * The {@link CloseWebSocketFrame} will be delivered to this method, but {@code ctx.close()}
     * will be called even if implementations ignore the {@link CloseWebSocketFrame}.
     * <p>
     * <strong>This method runs within the event loop. Do not block.</strong>
     *
     * @param frame a {@link WebSocketFrame} object.
     */
    void frame(WebSocketFrame frame);
}
