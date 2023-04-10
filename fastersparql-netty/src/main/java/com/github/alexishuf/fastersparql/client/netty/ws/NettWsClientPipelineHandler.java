package com.github.alexishuf.fastersparql.client.netty.ws;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;

import static io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory.newHandshaker;
import static io.netty.handler.codec.http.websocketx.WebSocketVersion.V13;
import static java.util.Objects.requireNonNull;

/**
 * Netty {@link ChannelInboundHandler} that handles handshake and feeds a {@link NettyWsClientHandler}.
 */
public class NettWsClientPipelineHandler extends SimpleChannelInboundHandler<Object> implements ChannelRecycler {
    private static final Logger log = LoggerFactory.getLogger(NettWsClientPipelineHandler.class);
    private final WebSocketClientHandshaker hs;
    private final ChannelRecycler recycler;
    private @Nullable NettyWsClientHandler delegate;
    private @MonotonicNonNull ChannelHandlerContext ctx;
    private boolean handshakeStarted, handshakeComplete, attached, detached;
    private Throwable earlyFailure;


    public NettWsClientPipelineHandler(URI uri, HttpHeaders headers, ChannelRecycler releaser) {
        this.hs = newHandshaker(uri, V13, null, true, headers);
        this.recycler = releaser;
    }

    public void delegate(NettyWsClientHandler delegate) {
        if (ctx == null || ctx.executor().inEventLoop()) {
            if (this.delegate != null && this.delegate != delegate) {
                String msg = "Already has delegate=" + this.delegate + ", cannot set " + delegate;
                throw new IllegalStateException(msg);
            }
            this.delegate = delegate;
            if (earlyFailure != null)
                detach(earlyFailure);
            else if (ctx != null)
                tryHandshake();
        } else if (ctx != null) {
            ctx.executor().execute(() -> delegate(delegate));
        }
    }

    private void tryHandshake() {
        check(delegate != null);
        if (!handshakeStarted) {
            handshakeStarted = true;
            hs.handshake(ctx.channel());
        } else if (handshakeComplete) {
            attach();
        }
    }

    private void check(boolean ok) {
        if (ctx != null && !ctx.executor().inEventLoop())
            throw new IllegalStateException(this+" not running in event loop!");
        if (!ok)
            throw new IllegalStateException(this+" bad state");
    }

    private void attach() {
        check(delegate != null && ctx != null && !attached && handshakeComplete);
        attached = true;
        delegate.attach(ctx, this);
    }

    private void detach(Throwable cause) {
        if (delegate != null) {
            check(true);
            if (attached || cause != null)
                delegate.detach(cause);
            else
                delegate.detach(new FSServerException("Could not establish WebSocket session"));
            delegate = null;
            attached = false;
            detached = true;
        }
    }

    @Override public void recycle(Channel channel) {
        if (ctx == null || ctx.channel() != channel)
            throw new IllegalStateException(this+".recycle("+channel+"): bogus channel");
        detach(null);
        recycler.recycle(channel);
    }

    @Override public void    handlerAdded(ChannelHandlerContext ctx) { this.ctx = ctx; }
    @Override public void   channelActive(ChannelHandlerContext ctx) { if (delegate != null) tryHandshake(); }
    @Override public void channelInactive(ChannelHandlerContext ctx) { detach(null); }

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
                exceptionCaught(ctx, new FSServerException(e.getMessage() + " uri="+hs.uri()));
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
                check(attached);
                delegate.frame((WebSocketFrame) msg);
            }
            if (msg instanceof CloseWebSocketFrame) {
                if (channel.isOpen())
                    ctx.close();
                log.debug("Closing {} due to CloseWebSocketFrame", channel);
            }
        } else {
            String type = msg == null ? "null" : msg.getClass().getSimpleName();
            throw new IllegalStateException("Unexpected "+type+" after websocket handshake");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if      (delegate != null)     detach(cause);
        else if (detached)             log.debug("{}: ignoring {} after detach()", this, Objects.toString(cause));
        else if (earlyFailure == null) earlyFailure = cause;
        try {
            ctx.close();
        } catch (Throwable e) { log.error("{}: ctx.close() failed: ", this, e); }
    }

    @Override public String toString() {
        return "NettyWsClientHandler{"
                + "handshake="+(handshakeComplete?"completed":(handshakeStarted?"":"!")+"started")
                + ", "+(attached?"":"!")+"attached"
                + ", "+(detached?"":"!")+"detached"
                + ", earlyFailure="+(earlyFailure == null ? "null": earlyFailure.getClass().getSimpleName())
                + ", ch="+(ctx == null ? "null" : ctx.channel())
                + ", delegate="+delegate+'}';
    }
}
