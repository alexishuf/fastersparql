package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.rope.BufferRope;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCounted;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;

import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpStatusClass.SUCCESS;
import static java.lang.invoke.MethodHandles.lookup;

public abstract class NettyHttpHandler extends SimpleChannelInboundHandler<HttpObject> {
    /**
     * Whether {@link #error(Throwable)} or {@link #content(HttpContent)}
     * (with {@link LastHttpContent}) have been called since last {@link #expectResponse()} call
     */
    private static final VarHandle TERMINATED;
    static {
        try {
            TERMINATED = lookup().findVarHandle(NettyHttpHandler.class, "plainTerminated", boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final Logger log = LoggerFactory.getLogger(NettyHttpHandler.class);

    private ChannelRecycler recycler = ChannelRecycler.CLOSE;
    protected @MonotonicNonNull ChannelHandlerContext ctx;
    private @Nullable HttpResponse httpResponse;
    private ByteRope failureBody = ByteRope.EMPTY;
    @SuppressWarnings("unused") // accessed through TERMINATED
    private boolean plainTerminated;

    public void recycler(ChannelRecycler recycler) {
        this.recycler = recycler;
    }

    /**
     * Enables notofication of events through {@link #successResponse(HttpResponse)},
     * {@link #error(Throwable)}.
     */
    public void expectResponse() {
        ChannelHandlerContext ctx = this.ctx;
        if (ctx != null)
            ctx.channel().config().setAutoRead(true);
        if ((boolean)TERMINATED.compareAndExchangeRelease(this, true, false)) {
            httpResponse = null;
            if (failureBody != ByteRope.EMPTY)
                failureBody.clear();
        }
    }

    /**
     * Called once a {@link HttpStatusClass#SUCCESS} response starts (status and headers arrived,
     * but {@link HttpContent}s chunks may follow.
     *
     * <p>After this call, chunks will be delivered via {@link #content(HttpContent)} calls
     * with the last call being a instance of {@link LastHttpContent}. If the response arrives
     * from the server as a single message (i.e., not {@code chunked} {@code Transfer-Encoding}),
     * {@link #content(HttpContent)} will be called with the same {@code response} object given
     * to this call</p>
     */
    protected abstract void successResponse(HttpResponse response);

    /**
     * Called when received a non-{@code SUCCESS} response or if no response arrived nor will
     * arrive (i.e., connection closed or local exception leading to an immediate connection
     * closure).
     *
     * @param cause non-null reason for termination. This may be an exception thrown locally
     *              or may be built from the status and body of a non-{@code SUCCESS} response.
     */
    protected abstract void error(Throwable cause);

    /**
     * Called after a {@link #successResponse(HttpResponse)} call for each chunk of the response.
     *
     * <p>This method may be called more than once. In any case the last call of a response
     * will have a {@link LastHttpContent} as its argument.</p>
     *
     * <p>If the server does not use {@code Transfer-Encoding: chunked}, this will be called
     * with the same {@link FullHttpResponse} given to {@link #successResponse(HttpResponse)}.</p>
     *
     * @param content a chunk of the response body.
     */
    protected abstract void content(HttpContent content);

    private boolean fail(@Nullable Throwable cause) {
        if ((boolean)TERMINATED.getAcquire(this)) return false;
        assert ctx == null || ctx.executor().inEventLoop() : "not in event loop";
        if (cause == null) {
            if (httpResponse != null) {
                cause = new FSServerException(httpResponse.status()+failureBody.toString())
                        .shouldRetry(httpResponse.status() == SERVICE_UNAVAILABLE);
            } else {
                cause = new FSException("Unknown failure");
            }
        }
        try {
            error(cause);
        } catch (Throwable t) {
            log.warn("Ignoring exception from {}.error()", this, t);
        }

        if (httpResponse instanceof ReferenceCounted r)
            r.release();
        httpResponse = null;
        failureBody = new ByteRope();
        TERMINATED.setRelease(this, true);
        ChannelHandlerContext ctx = this.ctx;
        if (ctx != null) {
            Channel ch = ctx.channel();
            if (ch != null) {
                ch.config().setAutoRead(true);
                if (ch.isActive()) ctx.close();
            }
        }
        return true;
    }


    @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        super.channelRegistered(ctx);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!(boolean)TERMINATED.getAcquire(this)) {
            String msg = "Connection closed before server"
                    + (httpResponse == null ? "started a response" : "completed the response");
            fail(new FSServerException(msg));
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!fail(cause) && ctx.channel().isActive())
            ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if ((boolean)TERMINATED.getAcquire(this)) {
            log.error("Ignoring post-terminated message {}", msg);
            return;
        }
        HttpResponse response = httpResponse;
        if (msg instanceof HttpResponse r) {
            if (response != null)
                throw new IllegalStateException("Unexpected response, already handling one");
            httpResponse = response = r;
            if (r.status().codeClass() == SUCCESS)
                successResponse(r);
        }
        HttpContent httpContent = msg instanceof HttpContent c ? c : null;
        if (httpContent != null) {
            if (response == null)
                throw new IllegalStateException("received HttpContent before HttpResponse");
            boolean isLast = httpContent instanceof LastHttpContent;
            if (response.status().codeClass() == SUCCESS) {
                content(httpContent);
                if (isLast) {
                    TERMINATED.setRelease(this, true);
                    httpResponse = null;
                    Channel ch = ctx.channel();
                    recycler.recycle(ch);
                }
            } else {
                if (failureBody == ByteRope.EMPTY)
                    failureBody = new ByteRope();
                failureBody.append(new BufferRope(httpContent.content().nioBuffer()));
                if (isLast)
                    fail(null); // will build a FSServerException from the response
            }
        }
    }
}
