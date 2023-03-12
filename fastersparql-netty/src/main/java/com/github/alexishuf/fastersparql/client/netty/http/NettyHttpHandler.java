package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class NettyHttpHandler extends SimpleChannelInboundHandler<HttpObject> {
    private ChannelRecycler recycler = ChannelRecycler.CLOSE;
    protected @MonotonicNonNull ChannelHandlerContext ctx;
    protected int responses, responseBytes;
    private boolean responseEnded, responseStarted;

    public void recycler(ChannelRecycler recycler) {
        this.recycler = recycler;
    }


    protected abstract void response(HttpResponse response);
    protected abstract void content(HttpContent content);

    /**
     * Called when the response started on {@link NettyHttpHandler#response(HttpResponse)}
     * completed due to the receipt of a {@link LastHttpContent} message
     * (that could be the {@link HttpResponse} itself), when the server closes the connection
     * before completing the message or when an error is thrown by Netty or this handler.
     *
     * <p>An implementation must do one of the following:</p>
     * <ol>
     *     <li>Issue a new request with {@code ctx.writeAndFlush(...)}</li>
     *     <li>Close the channel with {@code ctx.close()}</li>
     *     <li>Recycle the channel allowing its use with new unrelated requests by calling
     *         {@link NettyHttpHandler#recycle()}</li>
     * </ol>
     *
     * <p>If an implementation does none of the above, the connection will remain idle until
     * either the server or netty close it due to lack of activity.</p>
     *
     * @param error if {@code null} indicates the reposnse ended due to an {@link LastHttpContent},
     *              else indicates the reason.
     */
    protected abstract void responseEnd(@Nullable Throwable error);

    @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.channelRegistered(ctx);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (responseStarted && !responseEnded) {
            responseEnded = true;
            String msg = "Connection closed before "
                       + (responses > 0 ? "completing the response"
                                        : "server started an HTTP response");
            responseEnd(new FSServerException(msg));
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!responseEnded) {
            responseEnded = true;
            responseEnd(cause);
        }
        if (ctx.channel().isOpen())
            ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        HttpContent httpContent = msg instanceof HttpContent c ? c : null;
        if (msg instanceof HttpResponse response) {
            ++responses;
            responseBytes = 0;
            responseEnded = false;
            responseStarted = true;
            response(response);
        }
        if (httpContent != null && !responseEnded) {
            responseBytes = httpContent.content().readableBytes();
            content(httpContent);
            if (msg instanceof LastHttpContent) {
                responseEnd(null);
                responseEnded = true;
            }
        }
    }

    protected void recycle() {
        responses = responseBytes = 0;
        responseStarted = responseEnded = false;
        if (ctx != null) {
            Channel ch = ctx.channel();
            if (ch.isOpen())
                this.recycler.recycle(ch);
        }
    }
}
