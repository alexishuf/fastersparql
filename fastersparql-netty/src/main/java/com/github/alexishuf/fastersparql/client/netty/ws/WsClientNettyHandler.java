package com.github.alexishuf.fastersparql.client.netty.ws;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.concurrent.EventExecutor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;

import static io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory.newHandshaker;
import static io.netty.handler.codec.http.websocketx.WebSocketVersion.V13;
import static java.util.Objects.requireNonNull;

/**
 * Netty {@link ChannelInboundHandler} that handles handshake and feeds a {@link WsClientHandler}.
 */
@Slf4j
public class WsClientNettyHandler extends SimpleChannelInboundHandler<Object> {
    private final WebSocketClientHandshaker hs;
    private final WsRecycler recycler;
    private @Nullable WsClientHandler delegate;
    private @MonotonicNonNull ChannelHandlerContext ctx;
    private boolean handshakeStarted = false;

    public WsClientNettyHandler(URI uri, HttpHeaders headers, WsRecycler recycler) {
        this.hs = newHandshaker(uri, V13, null, true, headers);
        this.recycler = ch -> {
            delegate = null;
            recycler.recycle(ch);
        };
    }

    public void delegate(WsClientHandler delegate) {
        if (this.delegate != null && this.delegate != delegate) {
            String msg = "Already has delegate="+this.delegate+", cannot set "+delegate;
            throw new IllegalStateException(msg);
        }
        this.delegate = delegate;
        EventExecutor el = ctx.executor();
        if (el.inEventLoop()) tryHandshake();
        else                  el.execute(this::tryHandshake);
    }

    private void tryHandshake() {
        assert ctx != null : "No ctx";
        assert ctx.executor().inEventLoop() : "Called from outside the event loop";
        Channel channel = ctx.channel();
        if (!handshakeStarted && delegate != null && channel.isOpen()) {
            handshakeStarted = true;
            hs.handshake(channel);
        }
    }

    private void detach() {
        if (delegate != null) {
            delegate.detach();
            delegate = null;
        }
    }

    @Override public void    handlerAdded(ChannelHandlerContext ctx) { this.ctx = ctx; }
    @Override public void   channelActive(ChannelHandlerContext ctx) { tryHandshake(); }
    @Override public void channelInactive(ChannelHandlerContext ctx) { detach(); }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, @Nullable Object msg) {
        Channel channel = ctx.channel();
        assert this.ctx.channel() == ctx.channel();
        if (!hs.isHandshakeComplete()) {
            try {
                hs.finishHandshake(channel, (FullHttpResponse) requireNonNull(msg));
                assert delegate != null : "null delegate";
                delegate.attach(ctx, recycler);
            } catch (WebSocketHandshakeException e) {
                exceptionCaught(ctx, e);
            }
        } else if (msg instanceof WebSocketFrame) {
            if (delegate == null) {
                if (msg instanceof TextWebSocketFrame) {
                    log.warn("{} Received TextWebSocketFrame on without delegate: {}",
                             this, ((TextWebSocketFrame) msg).text());
                } else if (!(msg instanceof CloseWebSocketFrame)) {
                    log.warn("{}, without delegate, received {}", this, msg);
                }
            } else {
                delegate.onFrame((WebSocketFrame) msg);
            }
            if (msg instanceof CloseWebSocketFrame) {
                if (channel.isOpen())
                    ctx.close();
                log.debug("Closing {} due to CloseWebSocketFrame", channel);
            }
        } else {
            String type = msg == null ? "null" : msg.getClass().getSimpleName();
            String errorMsg = "Unexpected "+type+" after websocket handshake";
            exceptionCaught(ctx, new IllegalStateException(errorMsg));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        try {
            ctx.close();
        } catch (Throwable e) {
            log.error("{}: ctx.close() failed: ", this, e);
        }
        if (delegate == null) {
            log. warn("{}, without delegate: ", this, cause);
        } else {
            log.debug("{}, without delegate: ", this, cause);
            delegate.onError(cause);
        }
    }

    @Override public String toString() {
        return "WsClientNettyHandler{" + (ctx == null ? "[detached]" : ctx.channel()) + "}";
    }
}
