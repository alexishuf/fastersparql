package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelBound;
import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.util.concurrent.BitsetRunnable;
import com.github.alexishuf.fastersparql.util.concurrent.LongRenderer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static io.netty.handler.codec.http.HttpStatusClass.SUCCESS;

/**
 * Handles netty events and delivers higher-level events via {@code on*()} methods.
 *
 * <p>The typical lifecycle is:</p>
 *
 * <ol>
 *     <li>{@link #start(ChannelRecycler, HttpRequest)} gets called to start a HTTP request.
 *     Concurrent requests are not allowed and will be rejected with an
 *     {@link IllegalStateException}</li>
 *     <li>One of the following happens:
 *         <ul>
 *             <li>{@link #onCancelled(boolean)} with {@code empty=true} if a {@link #cancel(int)}
 *                 arrived from any thread and was serialized in the event loop before the
 *                 request got sent.</li>
 *             <li>{@link #onSuccessResponse(HttpResponse)} if the server starts a 200 response.
 *                 This will be followed by:
 *                 <ol>
 *                     <li>zero or more {@link #onContent(HttpContent)} calls</li>
 *                     <li>Either:
 *                         <ul>
 *                             <li>{@link #onSuccessLastContent()}</li>
 *                             <li>{@link #onCancelled(boolean)} if there was a
 *                                 {@link #cancel(int)} and the channel closed before the
 *                                 response completed</li>
 *                             <li>{@link #onIncompleteSuccessResponse(boolean)} if the channel
 *                                 closes before receiving the {@link LastHttpContent} and
 *                                 the closing was not caused by {@link #cancel(int)} or
 *                                 by an uncaught exception</li>
 *                         </ul>
 *                     </li>
 *                 </ol>
 *             </li>
 *             <li>{@link #onFailureResponse(HttpResponse, ByteRope, boolean)} if the channel
 *                 closes before the server starts a response or if the server answers with a
 *                 non-200 response (which may be incomplete if the channel closes before
 *                 it completes)</li>
 *         </ul>
 *     </li>
 * </ol>
 *
 * <p>At any moment, {@link #onClientSideError(Throwable)} may be called to notify that an
 * exception has been thrown from the above methods or while handling netty events. After
 * {@link #onClientSideError(Throwable)} there will be no more {@code on*()} calls and
 * the channel will be closed without returning to the pool.</p>
 *
 * <p>All the {@code on*()} methods are called from the event loop thread that runs events
 * for the channel. {@link #start(ChannelRecycler, HttpRequest)} may be called from any
 * thread but must not be called concurrently.</p>
 */
public abstract class NettyHttpHandler extends SimpleChannelInboundHandler<HttpObject>
        implements ChannelBound {
    private static final Logger log = LoggerFactory.getLogger(NettyHttpHandler.class);
    private static final VarHandle COOKIE;
    static {
        try {
            COOKIE = MethodHandles.lookup().findVarHandle(NettyHttpHandler.class, "plainCookie", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private @MonotonicNonNull ChannelHandlerContext ctx;
    private int st = ST_CAN_REQUEST;
    private @Nullable HttpResponse httpResp;
    private ChannelRecycler recycler = ChannelRecycler.CLOSE;
    private final Actions actions = new Actions();
    private @MonotonicNonNull Channel lastCh;
    private @Nullable HttpRequest pendingRequest;
    private int doSendRequestCookie, doCancelCookie;
    @SuppressWarnings("FieldMayBeFinal") private int plainCookie = RECYCLED_COOKIE;
    private ByteRope failureBody = ByteRope.EMPTY;

    private static final int RECYCLED_COOKIE = 0x80000000;

    private static final int ST_CAN_REQUEST     = 0x001;
    private static final int ST_REQ_SENT        = 0x002;
    private static final int ST_TERMINATED      = 0x004;
    private static final int ST_OK_RESP         = 0x008;
    private static final int ST_FAIL_RESP       = 0x010;
    private static final int ST_FAIL_RESP_TRUNC = 0x020;
    private static final int ST_NON_EMPTY       = 0x040;
    private static final int ST_GOT_LAST_CHUNK  = 0x080;
    private static final int ST_CANCELLED       = 0x100;
    private static final int ST_UNHEALTHY       = 0x200;

    private static final LongRenderer ST = st -> {
        if (st == 0) return "";
        var sb = new StringBuilder().append('[');
        if ((st&ST_CAN_REQUEST)     != 0) sb.append("CAN_REQUEST,");
        if ((st&ST_REQ_SENT)        != 0) sb.append("REQ_SENT,");
        if ((st&ST_TERMINATED)      != 0) sb.append("TERMINATED,");
        if ((st&ST_OK_RESP)         != 0) sb.append("OK_RESP,");
        if ((st&ST_FAIL_RESP)       != 0) sb.append("FAIL_RESP,");
        if ((st&ST_FAIL_RESP_TRUNC) != 0) sb.append("FAIL_RESP_TRUNC,");
        if ((st&ST_NON_EMPTY)       != 0) sb.append("NON_EMPTY,");
        if ((st&ST_GOT_LAST_CHUNK)  != 0) sb.append("GOT_LAST_CHUNK,");
        if ((st&ST_CANCELLED)       != 0) sb.append("CANCELLED,");
        if ((st&ST_UNHEALTHY)       != 0) sb.append("UNHEALTHY,");
        sb.setLength(sb.length()-1);
        return sb.append(']').toString();
    };

    public NettyHttpHandler(Executor executor) {
        actions.executor(executor);
    }

    private void closeOrRecycleChannel() {
        var ctx = this.ctx;
        if (ctx == null)
            return;
        Channel ch = ctx.channel();
        int ck = (int)COOKIE.getAcquire(this);
        var r = recycler;
        recycler = ChannelRecycler.CLOSE;
        if (!ctx.executor().inEventLoop()) {
            badCloseOrRecycleChannel("outside event loop");
        } else if (!ch.isOpen()) {
            journal("closeOrRecycleChannel", this, "is already closed");
        } else if ((st&ST_UNHEALTHY|ST_TERMINATED) != ST_TERMINATED) {
            if ((st&ST_TERMINATED) == 0)
                badCloseOrRecycleChannel("before deliverTermination");
            else
                ctx.close();
        } else if ((ck&RECYCLED_COOKIE) != 0) {
            badCloseOrRecycleChannel("already recycled");
        } else if ((int)COOKIE.compareAndExchangeRelease(this, ck, ck|RECYCLED_COOKIE) != ck) {
            badCloseOrRecycleChannel("concurrent");
        } else {
            st = ST_CAN_REQUEST;
            ch.config().setAutoRead(true);
            r.recycle(ctx.channel());
        }
    }

    private void badCloseOrRecycleChannel(String reason) {
        journal("closeOrRecycleChannel", this, reason);
        var ex = new IllegalStateException(this+".closeOrRecycleChannel(): "+reason);
        log.error("closeOrRecycleChannel {} {}", this, reason, ex);
        deliverTermination(ex);
        if (ctx != null && ctx.channel().isActive())
            ctx.close();
    }

    /* --- --- --- cancel --- --- --- */

    /**
     * Cancel the HTTP request. This is done by not sending the request if it has not yet been
     * sent or by closing the channel if the request has been sent and the response has not
     * yet been completely received.
     *
     * <p>This method may be called from any thread at any time. A cancel will be scheduled for
     * execution in the channel event loop. </p>
     *
     * @param cookie the value returned by {@link #start(ChannelRecycler, HttpRequest)}. If this
     *               does not match the last cookie, the call will have no effect
     */
    public void cancel(int cookie) {
        if (cookie != (int)COOKIE.getAcquire(this)) {
            journal("cancel: bad cookie=", cookie, "st=", st, ST, "on", this);
            return;
        }
        doCancelCookie = cookie;
        actions.sched(AC_CANCEL);
    }
    @SuppressWarnings("unused") private void doCancel() {
        int cookie = doCancelCookie;
        if (cookie != (int)COOKIE.getAcquire(this)) {
            journal("doCancel: bad cookie=", cookie, "st=", st, ST, "on", this);
            return;
        }
        if ((st&ST_TERMINATED) != 0)         // terminated
            return;                          // nothing to cancel or notify
        if (ctx != null)
            ctx.channel().config().setAutoRead(true);
        if ((st&ST_REQ_SENT) != 0) {         // request sent
            st |= ST_CANCELLED|ST_UNHEALTHY; // allow onCancelled, forbid recycling
            if (ctx != null) ctx.close();    // else: channelInactive() already delivered
        } else {                             // request not sent
            st |= ST_CANCELLED;              // call onCancelled(), but not from here
            deliverTermination(null);
        }
    }

    /**
     * Equivalent to {@link #cancel(int)} with the cookie that
     * {@link #start(ChannelRecycler, HttpRequest)} would return.
     *
     * @throws IllegalStateException if called after {@link #start(ChannelRecycler, HttpRequest)}
     */
    public void cancelBeforeStart() {
        int cookie = (int)COOKIE.getAcquire(this);
        if ((cookie&RECYCLED_COOKIE) == 0
                || (st&(ST_REQ_SENT|ST_TERMINATED|ST_CAN_REQUEST)) != ST_CAN_REQUEST) {
            throw new IllegalStateException("start() already called");
        }
        doCancelCookie = cookie;
        actions.sched(AC_CANCEL_BEFORE_START);
    }
    @SuppressWarnings("unused") private void doCancelBeforeStart() {
        int ck = doCancelCookie;
        if (ck != (int)COOKIE.getAcquire(this) || (ck&RECYCLED_COOKIE) == 0
                || (st&ST_REQ_SENT) != 0 ) {
            journal("doCancelBeforeStart: bad", ck, "st=", st, ST, "on", this);
            return;
        }
        if ((int)COOKIE.compareAndExchangeRelease(this, ck, ck&~RECYCLED_COOKIE) != ck) {
            journal("doCancelBeforeStart: start() happened st=", st, ST, "on", this);
            log.error("concurrent start() and cancelBeforeStart on {}", this);
            return;
        }
        st |= ST_CANCELLED;
        deliverTermination(null);
    }

    /* --- --- --- request start --- --- --- */

    /**
     * send the given {@link HttpRequest} and when a success response is fully received and
     * handled, recycle the channel with the given {@link ChannelRecycler}.
     *
     * <p><strong>Important:</strong> this method MUST only be called after the channel has
     * been created or acquired from the pool</p>
     *
     * @param recycler what to do with the channel once the response has been fully received and handled.
     * @param request the {@link HttpRequest} to send
     * @return an usage cookie
     */
    public int start(ChannelRecycler recycler, HttpRequest request) {
        int frozen = (int)COOKIE.getAcquire(this);
        int cookie = ((frozen&~RECYCLED_COOKIE) + 1)&~RECYCLED_COOKIE;
        if ((frozen&RECYCLED_COOKIE) == 0 || pendingRequest != null
                || (st&(ST_UNHEALTHY|ST_CAN_REQUEST)) != ST_CAN_REQUEST
                || (int)COOKIE.compareAndExchangeRelease(this, frozen, cookie) != frozen) {
            throw cannotSendRequest(frozen);
        }
        this.recycler       = recycler;
        doSendRequestCookie = cookie;
        pendingRequest      = request;
        actions.sched(AC_ENABLE_AUTOREAD|AC_SEND_REQUEST);
        return cookie;
    }
    @SuppressWarnings("unused") private void doSendRequest() {
        int cookie     = doSendRequestCookie;
        var request    = pendingRequest;
        pendingRequest = null;
        try {
            if (cookie != (int)COOKIE.getAcquire(this)) {
                journal("doSendRequest: bad cookie", cookie, "st=", st, ST, "on", this);
                return;
            }
            if ((st&(ST_UNHEALTHY|ST_CAN_REQUEST)) != ST_CAN_REQUEST)
                throw cannotSendRequest(cookie);
            st = (st&~ST_CAN_REQUEST) | ST_REQ_SENT;
            ctx.writeAndFlush(request);
            request = null;
        } finally {
            if (request instanceof HttpContent hc && hc.refCnt() > 0)
                hc.release(); // no writeAndFlush(request)
        }
    }

    private IllegalStateException cannotSendRequest(int cookie) {
        String reason;
        if (pendingRequest != null)
            reason = ": already has a pending request";
        else if ((st&(ST_REQ_SENT|ST_TERMINATED)) == ST_REQ_SENT)
            reason = ": already has an active request";
        else if ((st&ST_UNHEALTHY) != 0)
            reason = ": channel was deemed unhealthy";
        else if ((cookie&RECYCLED_COOKIE) != 0)
            reason = ": channel already in use elsewhere";
        else if ((st&ST_CAN_REQUEST) == 0)
            reason = ": CAN_REQUEST bit is off";
        else
            reason = ": unknown reason, FIXME";
        return new IllegalStateException(this+reason);
    }

    /* --- --- --- auto read --- --- ---  */

    /** Enables {@link ChannelConfig#isAutoRead()}. */
    public void enableAutoRead()  { setAutoRead(true, AC_ENABLE_AUTOREAD); }
    /** Disables {@link ChannelConfig#isAutoRead()} */
    public void disableAutoRead() { setAutoRead(false, AC_DISABLE_AUTOREAD); }
    private void setAutoRead(boolean value, int action) {
        journal(value ? "enableAutoRead, st=" : "disableAutoRead, st=", st, ST, "on", this);
        actions.sched(action);
    }
    private void doSetAutoRead(boolean value) {
        if (ctx != null) {
            ctx.channel().config().setAutoRead(value);
            journal("doSetAutoRead", value?1:0, "st=", st, ST, "on", this);
        }
    }
    @SuppressWarnings("unused") private void doEnableAutoRead() { doSetAutoRead(true); }
    @SuppressWarnings("unused") private void doDisableAutoRead() { doSetAutoRead(false); }

    /* --- --- --- actions --- --- ---  */

    private static final BitsetRunnable.Spec ACTIONS_SPEC;
    private static final int AC_CANCEL_BEFORE_START;
    private static final int AC_CANCEL;
    private static final int AC_DISABLE_AUTOREAD;
    private static final int AC_ENABLE_AUTOREAD;
    private static final int AC_SEND_REQUEST;

    static {
        var s = new BitsetRunnable.Spec(MethodHandles.lookup());
        AC_CANCEL_BEFORE_START = s.add("doCancelBeforeStart", true);
        AC_CANCEL              = s.add("doCancel",            true);
        AC_DISABLE_AUTOREAD    = s.add("doDisableAutoRead",   false);
        AC_ENABLE_AUTOREAD     = s.add("doEnableAutoRead",    false);
        AC_SEND_REQUEST        = s.add("doSendRequest",       true);
        ACTIONS_SPEC = s;
    }

    private class Actions extends BitsetRunnable<NettyHttpHandler> {
        public Actions() {super(NettyHttpHandler.this, ACTIONS_SPEC);}

        @Override protected void onMethodError(String methodName, Throwable t) {
            super.onMethodError(methodName, t);
            exceptionCaught(ctx, t);
        }
    }

    /* --- --- --- ChannelBound --- --- ---  */

    @Override public final @Nullable Channel channelOrLast() { return lastCh; }

    @Override public final void setChannel(Channel ch) {
        if (ch != channelOrLast()) throw new UnsupportedOperationException();
    }

    @Override public String journalName() {
        return "NHH:"+(lastCh == null ? "null" : lastCh.id().asShortText());
    }

    @Override public String toString() {return journalName()+ST.render(st);}

    /* --- --- --- events to be handled by a subclass --- --- --- */

    /**
     * Called once a {@link HttpStatusClass#SUCCESS} response starts (status and headers arrived),
     * but {@link HttpContent}s chunks may follow.
     *
     * <p>See {@link NettyHttpHandler} for the lifecycle overview including which
     * events may happen after this.</p>
     */
    protected abstract void onSuccessResponse(HttpResponse response);

    /**
     * Called if the channel gets closed before a response arrives or if the server responds
     * with a non-200 {@link HttpResponse}.
     *
     * <p>See {@link NettyHttpHandler} for the lifecycle overview including which
     * events may happen after this.</p>
     *
     * @param response The {@link HttpResponse} sent by the server or {@code null}
     *                 if the channel closed before a response arrived.
     * @param body If non-null, the body of the non-200 response. if the server replied with
     *             chunks they will be aggregated
     * @param bodyComplete if {@code true} {@code body} contains the full response
     */
    protected abstract void onFailureResponse(@Nullable HttpResponse response,
                                              @Nullable ByteRope body,
                                              boolean bodyComplete);

    /**
     * Called when any {@code on*()} method throws an exception or if an error is raised by
     * netty at the client side. This does not include non-200 responses received from
     * the server.
     *
     * <p>See {@link NettyHttpHandler} for the lifecycle overview including which
     * events may happen after this.</p>
     *
     * @param cause non-null reason for termination.
     */
    protected abstract void onClientSideError(Throwable cause);

    /**
     * Called zero or more times after a {@link #onSuccessResponse(HttpResponse)}, once per
     * {@link HttpContent}, including the {@link LastHttpContent}.
     *
     * <p>If the server does not use {@code Transfer-Encoding: chunked}, this will be called
     * with the same {@link FullHttpResponse} given to {@link #onSuccessResponse(HttpResponse)}.</p>
     *
     * <p>See {@link NettyHttpHandler} for the lifecycle overview including which
     * events may happen after this.</p>
     *
     * @param content a chunk of the response body.
     */
    protected abstract void onContent(HttpContent content);

    /**
     * Called once after {@link #onSuccessResponse(HttpResponse)} after the {@link LastHttpContent}
     * is received.
     *
     * <p>See {@link NettyHttpHandler} for the lifecycle overview including which
     * events may happen after this.</p>
     */
    protected abstract void onSuccessLastContent();

    /**
     * Called at most once after {@link #onSuccessResponse(HttpResponse)} if the channel becomes
     * inactive before {@link #onSuccessLastContent()}.
     *
     * <p>See {@link NettyHttpHandler} for the lifecycle overview including which
     * events may happen after this.</p>
     *
     * @param empty if the response had zero content bytes, despite having a success status code.
     */
    protected abstract void onIncompleteSuccessResponse(boolean empty);

    /**
     * Called at most once after a {@link #cancel(int)}, if the cancellation is enacted before
     * the request is sent or if the channel closes before a success response completes.
     *
     * <p>This indicates that the results were not completely received, either because the request
     * was never sent or because the channel got closed by the cancellation before the
     * response arrived.</p>
     *
     * @param empty {@code true} if there was no non-empty {@link #onContent(HttpContent)}
     *                         before this event.
     */
    protected abstract void onCancelled(boolean empty);

    /* --- --- --- handle netty events --- --- ---*/

    @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        this.ctx = ctx;
        journal("channelRegistered", this, "ctx=", ctx);
        actions.executor(ctx.executor());
        lastCh = ctx.channel();
    }

    @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        actions.runNow();
        actions.executor(ForkJoinPool.commonPool());
        this.ctx = null;
    }

    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        actions.runNow();
    }

    private void deliverTermination(@Nullable Throwable error) {
        if ((st&ST_TERMINATED) != 0)
            return;
        st |= ST_TERMINATED;
        try {
            if (error != null) {
                onClientSideError(error);
            } else if ((st&ST_FAIL_RESP) != 0) {
                st |= ST_UNHEALTHY;
                onFailureResponse(httpResp, failureBody, (st&ST_FAIL_RESP_TRUNC) == 0);
            } else if ((st&(ST_NON_EMPTY|ST_GOT_LAST_CHUNK)) == (ST_NON_EMPTY|ST_GOT_LAST_CHUNK)) {
                onSuccessLastContent();
            } else if ((st&ST_CANCELLED) != 0) {
                onCancelled((st&ST_NON_EMPTY) != 0);
            } else {
                st |= ST_UNHEALTHY;
                onIncompleteSuccessResponse((st&ST_NON_EMPTY) == 0);
            }
        } catch (Throwable t) {
            st |= ST_UNHEALTHY;
            log.error("Ignoring {} thrown during deliverTermination() for {}",
                      t.getClass().getSimpleName(), this, t);
        } finally {
            if (httpResp instanceof HttpContent c)
                c.release();
            httpResp = null;
        }
        if ((st&ST_UNHEALTHY) == 0)
            closeOrRecycleChannel();
        else if (ctx != null) {
            journal("deliverTermination", this, "closing unhealthy");
            ctx.close();
        }
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        actions.runNow();
        deliverTermination(null);
        super.channelInactive(ctx);
    }

    @Override
    public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        journal("exceptionCaught st=", st, ST, cause, "on", this);
        st |= ST_UNHEALTHY;
        deliverTermination(cause);
        if (ctx != null)
            ctx.close();
    }

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        actions.runNow();
        if ((st&ST_REQ_SENT) == 0) {
            readSpontaneous(msg);
            return;
        }
        if ((st&ST_TERMINATED) != 0) {
            readPostTerm(msg);
            return;
        }
        var httpResponse = this.httpResp;
        if (msg instanceof HttpResponse r) {
            if (httpResponse != null)
                throw new IllegalStateException("Unexpected response, already handling one");
            this.httpResp = httpResponse = r;
            if (r.status().codeClass() == SUCCESS) {
                st |= ST_OK_RESP;
                journal("successResponse() st=", st, ST, "on", this);
                onSuccessResponse(r);
            } else {
                st |= ST_FAIL_RESP;
                journal("got HTTP ", r.status().code(), "st=", st, ST, "on", this);
            }
        }
        if (msg instanceof HttpContent content) {
            if (httpResponse == null)
                throw new IllegalStateException("received HttpContent before HttpResponse");
            if ((st&ST_FAIL_RESP) != 0) {
                appendFailContent(content);
            } else if (content.content().readableBytes() > 0) {
                st |= ST_NON_EMPTY;
                onContent(content);
            }
            if (content instanceof LastHttpContent) {
                st |= ST_GOT_LAST_CHUNK;
                deliverTermination(null);
            }
        }
    }

    private void appendFailContent(HttpContent content) {
        if (failureBody == ByteRope.EMPTY)
            failureBody = new ByteRope();
        ByteBuf bb = content.content();
        int nBytes = bb.readableBytes();
        if (failureBody.len+nBytes > 4096) {
            st |= ST_FAIL_RESP_TRUNC;
        } else {
            failureBody.ensureFreeCapacity(nBytes);
            bb.readBytes(failureBody.u8(), failureBody.len, nBytes);
            failureBody.len += nBytes;
        }
    }

    private void readSpontaneous(HttpObject msg) {
        st |= ST_UNHEALTHY;
        ctx.close();
        if (msg instanceof HttpResponse res) {
            journal("spontaneous HTTP ", res.status().code(), " response st=",
                    st, ST, "on", this);
        } else {
            journal("spontaneous HTTP message, last=", msg instanceof LastHttpContent ? 1 : 0,
                    ", st=", st, ST, "on", this);
        }
        if (msg instanceof HttpContent c)
            journal("spontaneous message bytes=", c.content().readableBytes(), "on", this);
        log.warn("Ignoring spontaneous server-sent {} on {}: {}",
                 msg.getClass().getSimpleName(), this, msg);
    }

    private void readPostTerm(HttpObject msg) {
        st |= ST_UNHEALTHY;
        ctx.close();
        if (msg instanceof HttpResponse res) {
            journal("post-term HTTP ", res.status().code(), " response st=",
                    st, ST, "on", this);
        } else {
            journal("post-term HTTP message, last=", msg instanceof LastHttpContent ? 1 : 0,
                    ", st=", st, ST, "on", this);
        }
        if (msg instanceof HttpContent c)
            journal("post-term message bytes=", c.content().readableBytes(), "on", this);
        if ((st&(ST_CANCELLED)) == 0) {
            log.warn("Ignoring unexpected server-sent {} on {}: {}",
                     msg.getClass().getSimpleName(), this, msg);
        }
    }

}
