package com.github.alexishuf.fastersparql.client.netty.ws;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.concurrent.EventExecutor;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory.newHandshaker;
import static io.netty.handler.codec.http.websocketx.WebSocketVersion.V13;
import static java.util.Objects.requireNonNull;

/**
 * Netty {@link ChannelInboundHandler} that handles handshake and feeds a {@link WsClientHandler}.
 */
public class WsClientNettyHandler extends SimpleChannelInboundHandler<Object> implements WsRecycler {
    private static final Logger log = LoggerFactory.getLogger(WsClientNettyHandler.class);
    private final WebSocketClientHandshaker hs;
    private final WsRecycler recycler;
    private @Nullable WsClientHandler delegate;
    private @MonotonicNonNull ChannelHandlerContext ctx;
    private boolean handshakeStarted, handshakeComplete, attached;

    public WsClientNettyHandler(URI uri, HttpHeaders headers, WsRecycler recycler) {
        this.hs = newHandshaker(uri, V13, null, true, headers);
        this.recycler = recycler;
    }

    public void delegate(WsClientHandler delegate) {
        if (this.delegate != null && this.delegate != delegate) {
            String msg = "Already has delegate="+this.delegate+", cannot set "+delegate;
            throw new IllegalStateException(msg);
        }
        this.delegate = delegate;
        //noinspection resource
        EventExecutor el = ctx.executor();
        if (el.inEventLoop()) tryHandshake();
        else                  el.execute(this::tryHandshake);
    }

    private void tryHandshake() {
        assert ctx != null : "No ctx";
        //noinspection resource
        assert ctx.executor().inEventLoop() : "Called from outside the event loop";
        Channel channel = ctx.channel();
        if (delegate == null) {
            log.trace("Ignoring {}.tryHandshake() with delegate=null", this);
        } else if (!channel.isOpen()) {
            detach();
        } else if (!handshakeStarted) {
            handshakeStarted = true;
            hs.handshake(channel);
        } else if (handshakeComplete) {
            attach();
        } else {
            log.trace("Skipping {}.tryHandshake(): handshake in progress", this);
        }
    }

    private void attach() {
        if (delegate == null || ctx == null || attached || !handshakeComplete) {
            assert false : "Illegal state for attach()";
            log.error("Ignoring illegal {}.attach()", this);
        } else {
            attached = true;
            delegate.attach(ctx, this);
        }
    }

    private void detach() {
        //noinspection resource
        assert ctx == null || ctx.executor().inEventLoop() : "detach() from outside event loop";
        if (delegate != null) {
            if (attached) {
                delegate.detach();
            } else if (ctx == null) {
                delegate.onError(new IllegalStateException("detach() with ctx == null"));
            } else if (!ctx.channel().isOpen()) {
                String msg = "Server closed connection before completing WS handshake";
                delegate.onError(new SparqlClientServerException(msg));
            } else {
                String msg = "detach() before attach() with open channel=";
                delegate.onError(new IllegalStateException(msg +ctx.channel()));
            }
            delegate = null;
            attached = false;
        }
    }

    @Override public void recycle(Channel channel) {
        assert ctx == null || ctx.channel() == channel : "recycling extraneous channel";
        detach();
        recycler.recycle(channel);
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
                handshakeComplete = true;
                attach();
            } catch (WebSocketHandshakeException e) {
                exceptionCaught(ctx, new SparqlClientServerException(e.getMessage() + " uri="+hs.uri()));
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
        return "WsClientNettyHandler{delegate="+delegate+
               ", ch=" + (ctx == null ? "[detached]" : ctx.channel()) + "}";
    }
}
