package com.github.alexishuf.fastersparql.client.netty.ws;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelBound;
import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.util.concurrent.ArrayPool;
import com.github.alexishuf.fastersparql.util.concurrent.DebugJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory.newHandshaker;
import static io.netty.handler.codec.http.websocketx.WebSocketVersion.V13;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Netty {@link ChannelInboundHandler} that handles handshake and feeds a {@link NettyWsClientHandler}.
 */
public class NettWsClientPipelineHandler extends SimpleChannelInboundHandler<Object>
        implements ChannelRecycler, ChannelBound {
    private static final Logger log = LoggerFactory.getLogger(NettWsClientPipelineHandler.class);
    private final WebSocketClientHandshaker hs;
    private final ChannelRecycler recycler;
    private @Nullable NettyWsClientHandler delegate;
    private @MonotonicNonNull ChannelHandlerContext ctx;
    private boolean handshakeStarted, handshakeComplete, attached, detached;
    private byte @Nullable [] previewU8;
    private final Runnable onDelegate = this::onDelegate;
    private Throwable earlyFailure;


    public NettWsClientPipelineHandler(URI uri, HttpHeaders headers, ChannelRecycler releaser) {
        this.hs = newHandshaker(uri, V13, null, true, headers);
        this.recycler = releaser;
    }

    public void delegate(NettyWsClientHandler delegate) {
        if (this.delegate != null && this.delegate != delegate) {
            String msg = "Already has delegate=" + this.delegate + ", cannot set " + delegate;
            throw new IllegalStateException(msg);
        }
        this.delegate = delegate;
        var exec = ctx == null ? null : ctx.executor();
        if (exec != null && !exec.inEventLoop()) exec.execute(onDelegate);
        else                                     onDelegate();
    }

    private void onDelegate() {
        if (delegate == null)
            return;
        if (earlyFailure != null)
            detach(earlyFailure);
        else if (ctx != null)
            tryHandshake();
    }


    private void tryHandshake() {
        check(delegate != null);
        if (!handshakeStarted) {
            journal("starting handshake on", this);
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
        journal("attach on ", this, "delegate=", delegate);
        check(delegate != null && ctx != null && !attached && handshakeComplete);
        attached = true;
        delegate.attach(ctx, this);
    }

    private void detach(Throwable cause) {
        journal("detach on", this, "cause=", cause);
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

    /* --- --- --- implement ChannelBound --- --- --- */

    @Override public @Nullable Channel channel() { return ctx == null ? null : ctx.channel(); }

    @Override public void setChannel(Channel ch) {
        if (ch != channel()) throw new UnsupportedOperationException();
    }

    @Override public String journalName() {
        return "WPH:"+(ctx == null ? "null" : ctx.channel().id().asShortText());
    }

    /* --- --- --- implement ChannelRecycler --- --- --- */

    @Override public void recycle(Channel channel) {
        journal("recycle ch=", channel, "this=", this);
        if (ctx == null || ctx.channel() != channel)
            throw new IllegalStateException(this+".recycle("+channel+"): bogus channel");
        detach(null);
        recycler.recycle(channel);
    }

    /* --- --- --- implement SimpleChannelInboundHandler --- --- --- */

    @Override public void    handlerAdded(ChannelHandlerContext ctx) { this.ctx = ctx; }
    @Override public void   channelActive(ChannelHandlerContext ctx) {
        if (delegate != null) tryHandshake();
    }
    @Override public void channelInactive(ChannelHandlerContext ctx) {
        detach(null);
        if (previewU8 != null)
            previewU8 = ArrayPool.BYTE.offer(previewU8, previewU8.length);
    }

    private void logMessage(Object msg) {
        if (msg instanceof HttpResponse r) {
            journal("Received HTTP ", r.status().code(), "on", this);
        } else if (msg instanceof CloseWebSocketFrame) {
            journal("Received close WS frame on", this);
        } else if (msg instanceof TextWebSocketFrame f) {
            ByteBuf bb = f.content();
            byte[] previewU8 = this.previewU8;
            if (previewU8 == null)
                this.previewU8 = previewU8 = ArrayPool.bytesAtLeast(19);
            int frameLen = bb.readableBytes(), previewLen = Math.min(frameLen, 16);
            bb.getBytes(bb.readerIndex(), this.previewU8, 0, previewLen);
            while (previewLen > 0 && previewU8[previewLen-1] <= ' ')
                --previewLen;
            if (frameLen > 16) {
                previewU8[previewLen++] = '.';
                previewU8[previewLen++] = '.';
                previewU8[previewLen++] = '.';
            }
            journal("Received WS frame with ", frameLen, "bytes on", this);
            journal("preview=", new String(previewU8, 0, previewLen, UTF_8));
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, @Nullable Object msg) {
        if (ThreadJournal.ENABLED)
            logMessage(msg);
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
        } else if (msg instanceof WebSocketFrame frame) {
            if (delegate == null) {
                journal("frame without delegate on", this);
                if (msg instanceof TextWebSocketFrame tFrame) {
                    log.warn("Received TextWebSocketFrame without delegate on {}: {}",
                             this, tFrame.text());
                } else if (!(msg instanceof CloseWebSocketFrame)) {
                    log.warn("Received message without delegate on {}: {}", this, msg);
                }
            } else {
                check(attached);
                delegate.frame(frame);
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
        if (ThreadJournal.ENABLED) {
            String state = "delegate="+DebugJournal.DefaultRenderer.INSTANCE.renderObj(delegate)
                         + (detached ? " detached" : (attached ? " attached" : " before attach"));
            journal(cause.getClass().getSimpleName(), "on", this, state);
        }
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
