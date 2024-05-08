package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.batch.*;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.*;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.netty.util.*;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClientHandler;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.HasFillingBatch;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.async.CallbackEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.PooledSegmentRopeView;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.sparql.results.AbstractWsClientParser;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.BitsetRunnable;
import com.github.alexishuf.fastersparql.util.concurrent.LongRenderer;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.quickAppend;
import static com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink.MIN_HINT;
import static com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink.NORMAL_HINT;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.sparql.results.AbstractWsParser.*;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.lang.Math.min;
import static java.lang.Thread.onSpinWait;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NettyWsSparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(NettyWsSparqlClient.class);
    private static final boolean SEND_INFO = FSNettyProperties.channelInfo();

    private final NettyWsClient netty;

    private static SparqlEndpoint restrictConfig(SparqlEndpoint endpoint) {
        SparqlConfiguration request = endpoint.configuration();
        SparqlConfiguration offer = request.toBuilder()
                .clearMethods().method(SparqlMethod.WS)
                .clearResultsAccepts().resultsAccept(SparqlResultFormat.TSV)
                .build();
        String cause = null;
        if (!request.resultsAccepts().contains(SparqlResultFormat.TSV))
            cause = "Only TSV results are supported in the Websocket protocol";
        else if (!request.methods().contains(SparqlMethod.WS))
            cause = "NettyWebSocketSparqlClient only supports websocket method";
        else if (!request.headers().isEmpty() || !request.appendHeaders().isEmpty())
            cause = "There is no support for headers in the Websocket protocol";
        if (cause != null)
            throw new UnacceptableSparqlConfiguration(endpoint.uri(), offer, request, cause);
        return new SparqlEndpoint(endpoint.uri(), offer);
    }

    public NettyWsSparqlClient(SparqlEndpoint ep) {
        super(restrictConfig(ep));
        var headers = new DefaultHttpHeaders();
        var config = ep.configuration();
        for (Map.Entry<String, String> e : config.headers().entrySet())
            headers.add(e.getKey(), e.getValue());
        for (Map.Entry<String, List<String>> e : config.appendHeaders().entrySet())
            headers.add(e.getKey(), e.getValue());
        this.bindingAwareProtocol = true;
        try {
            this.netty = new NettyClientBuilder().buildWs(ep.protocol(), ep.toURI(), headers);
        } catch (SSLException e) {
            throw new FSException("Could not initialize SSL context", e);
        }
    }

    @Override public SparqlClient.Guard retain() { return new RefGuard(); }

    @Override protected void doClose() { netty.close(); }

    @Override public <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sparql) {
        return new WsBIt<>(bt, sparql.publicVars(), sparql, null);
    }

    @Override
    protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        return new WsEmitter<>(bt, sparql.publicVars(), sparql, null);
    }

    @Override public <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
        return new WsBIt<>(bq.batchType(), bq.resultVars(), bq.query, bq);
    }

    @Override protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(EmitBindQuery<B> query, Vars rebindHint) {
        return new WsEmitter<>(query.batchType(), query.resultVars(), query.query, query);
    }

    /* --- --- --- helper methods --- --- --- */

    private static final byte[] QUERY_VERB = "!query ".getBytes(UTF_8);
    private static final byte[][] BIND_VERB = {
            "!join "      .getBytes(UTF_8),
            "!left-join " .getBytes(UTF_8),
            "!exists "    .getBytes(UTF_8),
            "!not-exists ".getBytes(UTF_8),
            "!minus "     .getBytes(UTF_8),
    };

    /* --- --- --- BIt/Emitter implementations --- --- --- */

    private final class WsBIt<B extends Batch<B>> extends SPSCBIt<B> implements WsStreamNode<B> {
        private static final VarHandle BIND_REQUEST;
        static {
            try {
                BIND_REQUEST = MethodHandles.lookup().findVarHandle(WsBIt.class, "plainBindRequest", long.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private final SparqlQuery query;
        private final @Nullable ItBindQuery<B> bindQuery;
        private final WsHandler<B> h;
        @SuppressWarnings("unused") private long plainBindRequest;
        private long pendingRows;
        private @Nullable Channel lastCh;
        private final @Nullable Thread sendBindingsThread;

        public WsBIt(BatchType<B> batchType, Vars outVars, SparqlQuery query,
                     @Nullable ItBindQuery<B> bindQuery) {
            super(batchType, outVars);
            this.query     = query;
            this.bindQuery = bindQuery;
            this.h         = new WsHandler<>(new BItWsParser(bindQuery), this, bindQuery);
            if (bindQuery == null)
                sendBindingsThread = null;
            else
                sendBindingsThread = Thread.startVirtualThread(this::sendBindings);
            h.sendQuery();
            h.requestRows(pendingRows = maxItems);
        }

        /* --- --- --- StreamNode & ChannelBound --- --- --- */

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return bindQuery == null ? Stream.empty() : Stream.of(bindQuery.bindings);
        }

        @Override public String journalName() {
            return "C.WB:" + (lastCh == null ? "null" : lastCh.id().asShortText()) + '@' + id();
        }

        @Override protected StringBuilder minimalLabel() {
            return new StringBuilder().append(journalName());
        }

        @Override protected void appendToSimpleLabel(StringBuilder sb) {
            super.appendToSimpleLabel(sb);
            if (h.info != null) sb.append(" info=").append(h.info);
        }

        @Override public @Nullable Channel channelOrLast() {
            return lastCh;
        }

        @Override public void setChannel(Channel channel) {
            if (channel != null) {
                this.lastCh = channel;
                if (sendBindingsThread != null)
                    sendBindingsThread.setName(journalName());
            }
        }

        /* --- --- --- SPSCBIt --- --- --- */

        @Override protected void cleanup(@Nullable Throwable cause) {
            h.sendCancel();
            h.release();
            super.cleanup(cause);
        }

        @Override public boolean tryCancel() {
            boolean did = super.tryCancel();
            if (did && bindQuery != null) {
                bindQuery.bindings.tryCancel();
                Unparker.unpark(sendBindingsThread);
            }
            return did;
        }

        @Override public @This CallbackBIt<B> maxReadyItems(int n) {
            super.maxReadyItems(n);
            if (n > pendingRows)
                h.requestRows(n);
            return this;
        }

        @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
            var orphan = super.nextBatch(offer);
            if (orphan != null) {
                pendingRows -= Batch.peekTotalRows(orphan);
                if (pendingRows <= maxItems>>1 && notTerminated())
                    h.requestRows(pendingRows = maxItems);
            }
            return orphan;
        }

        @Override protected boolean mustPark(int offerRows, long queuedRows) {
            return false;
        }

        /* --- --- --- WsStreamNode --- --- --- */

        @Override public SegmentRope sparql() { return query.sparql(); }
        @Override public void beforeSendBindQuery() {}
        @Override public String renderState() { return state().name(); }
        @Override public String instanceId() {return String.valueOf(id());}

        /* --- --- --- parser --- --- --- */

        private final class BItWsParser extends WsParser<B> {
            public BItWsParser(ItBindQuery<B> bindQuery) {super(WsBIt.this, bindQuery);}

            @Override protected void handleBindRequest(long n) {
                if (Async.maxRelease(WsBIt.BIND_REQUEST, WsBIt.this, n))
                    Unparker.unpark(sendBindingsThread);
            }
            @Override protected void onPing() {h.sendPingAck();}
        }

        /* --- --- --- bindings sender --- --- --- */

        private void sendBindings() {
            Thread.currentThread().setName(journalName());
            assert bindQuery != null;
            Throwable termCause = null;
            try (var bindings = bindQuery.bindings) {
                while (true) {
                    long req;
                    while ((req=(long)BIND_REQUEST.getAcquire(this)) <= 0 && notTerminated())
                        LockSupport.park();
                    bindings.maxBatch((int) Math.min(Integer.MAX_VALUE, req));
                    var orphan = bindings.nextBatch(null);
                    if (orphan == null)
                        break;
                    BIND_REQUEST.getAndAddRelease(this, -(long)Batch.peekTotalRows(orphan));
                    h.sendBatch(orphan);
                }
            } catch (Throwable t) {
                termCause = t;
            } finally {
                journal("sendBindings done, reason", termCause == null ? "completion" : termCause,
                        "on", this);
                if (h.parser.noServerTermination()) {
                    journal("no term from server, sending bindings term on", this);
                    if (termCause instanceof BItReadCancelledException)
                        termCause = CancelledException.INSTANCE;
                    h.sendBindingTerm(termCause);
                }
            }
        }
    }

    private interface WsStreamNode<B extends Batch<B>>
            extends StreamNode, ChannelBound, CompletableBatchQueue<B> {
        SegmentRope sparql();
        void beforeSendBindQuery();
        String renderState();
        String instanceId();
    }

    private static abstract class WsParser<B extends Batch<B>> extends AbstractWsClientParser<B> {
        private WsHandler<B> h;
        public WsParser(CompletableBatchQueue<B> dst, @Nullable BindQuery<B> bindQuery) {
            super(dst, bindQuery);
            namer(NAMER, null);
        }

        private static final Namer<Object> NAMER = (parser, ignored) -> {
            WsParser<?> p = (WsParser<?>)parser;
            var sb = new StringBuilder().append(p.format().lowercase());
            sb.append(':').append(p.h.journalName());
            if (p.h.info != null)
                sb.append("<-").append(p.h.info);
            return sb.toString();
        };

        public boolean noServerTermination() { return !serverSentTermination; }

        @Override protected void onInfo(SegmentRope rope, int begin, int end) {
            super.onInfo(rope, begin, end);
            h.info = rope.toString(begin+INFO.length, end);
        }

        @Override protected void beforeComplete(@Nullable Throwable error) {
            super.beforeComplete(error);
            h.beforeParserCompletes();
        }
    }


    private class WsHandler<B extends Batch<B>>
            implements NettyWsClientHandler, ChannelBound, HasFillingBatch<B>,
                       ResultsSerializer.ChunkConsumer<ByteBuf> {
        private static final VarHandle BINDINGS_LOCK, REQ_ROWS, RELEASE_LOCK;
        static {
            try {
                BINDINGS_LOCK = MethodHandles.lookup().findVarHandle(WsHandler.class, "plainBindingsLock", int.class);
                RELEASE_LOCK = MethodHandles.lookup().findVarHandle(WsHandler.class, "plainReleaseLock", int.class);
                REQ_ROWS = MethodHandles.lookup().findVarHandle(WsHandler.class, "plainRequestRows", long.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private static final int REQ_ROWS_MSG_CAP = REQUEST.length + String.valueOf(Long.MAX_VALUE).length() + 1;
        private static final byte[] MAX_LF = "MAX\n".getBytes(UTF_8);

        private static final int ST_CONNECTING         = 0x0001;
        private static final int ST_ATTACHED           = 0x0002;
        private static final int ST_SEND_QUERY         = 0x0004;
        private static final int ST_QUERY_SENT         = 0x0008;
        private static final int ST_SEND_BATCH         = 0x0010;
        private static final int ST_SEND_BINDINGS_TERM = 0x0020;
        private static final int ST_BINDINGS_TERM_SENT = 0x0040;
        private static final int ST_SEND_CANCEL        = 0x0080;
        private static final int ST_CANCEL_SENT        = 0x0100;
        private static final int ST_GOT_FRAMES         = 0x0200;
        private static final int ST_GOT_TERM           = 0x0400;
        private static final int ST_CAN_RELEASE        = 0x0800;
        private static final int ST_RELEASED           = 0x1000;
        private static final int ST_RETRY              = 0x2000;

        private static final LongRenderer ST = st -> {
            var sb = new StringBuilder();
            if ((st&ST_CONNECTING)         != 0) sb.append("CONNECTING,");
            if ((st&ST_ATTACHED)           != 0) sb.append("ATTACHED,");
            if ((st&ST_SEND_QUERY)         != 0) sb.append("SEND_QUERY,");
            if ((st&ST_QUERY_SENT)         != 0) sb.append("QUERY_SENT,");
            if ((st&ST_SEND_BATCH)         != 0) sb.append("SEND_BATCH,");
            if ((st&ST_SEND_BINDINGS_TERM) != 0) sb.append("SEND_BINDINGS_TERM,");
            if ((st&ST_BINDINGS_TERM_SENT) != 0) sb.append("BINDINGS_TERM_SENT,");
            if ((st&ST_SEND_CANCEL)        != 0) sb.append("SEND_CANCEL,");
            if ((st&ST_CANCEL_SENT)        != 0) sb.append("CANCEL_SENT,");
            if ((st&ST_GOT_FRAMES)         != 0) sb.append("GOT_FRAMES,");
            if ((st&ST_GOT_TERM)           != 0) sb.append("GOT_TERM,");
            if ((st&ST_CAN_RELEASE)        != 0) sb.append("CAN_RELEASE,");
            if ((st&ST_RELEASED)           != 0) sb.append("RELEASED,");
            sb.setLength(Math.max(0, sb.length()-1));
            return sb.toString();
        };

        private final WsBitsetRunnable bsRunnable = new WsBitsetRunnable(this);
        private final WsParser<B> parser;
        private final ByteBufSink bbSink = new ByteBufSink(ByteBufAllocator.DEFAULT);
        private ByteBufRopeView bbView = new ByteBufRopeView(PooledSegmentRopeView.ofEmpty());
        private @Nullable ChannelHandlerContext ctx;
        private int st;
        private WsSerializer serializer;
        private final @Nullable BindType bindType;
        @SuppressWarnings("unused") private int plainBindingsLock;
        @SuppressWarnings("unused") private long plainRequestRows;
        private final MutableRope reqRowsMsg = new MutableRope(REQ_ROWS_MSG_CAP).append(REQUEST);
        private final byte[] reqRowsU8 = reqRowsMsg.u8();
        private final TextWebSocketFrame reqRowsFrame = new TextWebSocketFrame(wrappedBuffer(reqRowsU8));
        private @Nullable B receivedBindings;
        private @Nullable Throwable bindingsError;
        private ChannelRecycler channelRecycler = ChannelRecycler.CLOSE;
        private final Vars bindingsVars, usefulBindingsVars;
        private @Nullable String info;
        private final WsStreamNode<B> parent;
        private int retries;
        @SuppressWarnings("unused") private int plainReleaseLock;

        public WsHandler(WsParser<B> parser, WsStreamNode<B> parent,
                         @Nullable BindQuery<B> bindQuery) {
            parser.h = this;
            this.parser = parser;
            this.parent = parent;
            if (bindQuery == null) {
                serializer = null;
                bindingsVars = Vars.EMPTY;
                usefulBindingsVars = Vars.EMPTY;
                bindType = null;
            } else {
                bindingsVars = bindQuery.bindingsVars();
                usefulBindingsVars = parent.vars().intersection(bindingsVars);
                serializer = WsSerializer.create(NORMAL_HINT).takeOwnership(this);
                bindType = bindQuery.type;
            }
            bsRunnable.executor(netty.executor());
            acquireRef();
        }

        public void reset() {
            bsRunnable.sched(AC_RESET);
        }

        @SuppressWarnings("unused") private void doReset() {
            if ((st&(ST_QUERY_SENT|ST_GOT_TERM)) == ST_QUERY_SENT)
                throw new IllegalStateException("reset after query sent but before term received");
            st &= ST_RELEASED;
            parser.reset(parent);

            // recycle any leftover bindings
            doReleaseReceivedBindings();
        }

        /** Release the queue of bindings received from upstream (not from our server) */
        private void doReleaseReceivedBindings() {
            while ((int)BINDINGS_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            B batch = receivedBindings;
            receivedBindings = null;
            BINDINGS_LOCK.setRelease(this, 0);
            Batch.recycle(batch, this);
        }

        @SuppressWarnings("unused") private void doRetry() {
            if ((st&(ST_CAN_RELEASE|ST_RELEASED|ST_RETRY)) == ST_RETRY) {
                st = 0;
                parser.reset(parent);
                long req = parent instanceof WsEmitter<B> em ? em.requested() : Long.MAX_VALUE;
                REQ_ROWS.setRelease(this, req);
                bsRunnable.sched(AC_SEND_QUERY);
            } else {
                journal("skip doRetry st=", st, ST, "on", this);
            }
        }


        public void release() {
            int actions = (st&ST_CAN_RELEASE) == 0 ? AC_ALLOW_RELEASE : 0;
            if ((st&ST_RELEASED) == 0)
                actions |= AC_RELEASE;
            if (actions != 0)
                bsRunnable.sched(actions);
        }

        @SuppressWarnings("unused") private void doAllowRelease() { st |= ST_CAN_RELEASE; }

        @SuppressWarnings("unused") private void doRelease() {
            assert inEventLoop() : "not in event loop";
            while ((int)RELEASE_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                if (ctx == null) {
                    if ((st & ST_RELEASED) == 0) {
                        journal("doRelease", this, ": really release");
                        st |= ST_RELEASED;
                        try {
                            doReleaseReceivedBindings();
                            bbView.close();
                            bbSink.close();
                            if (reqRowsFrame.refCnt() > 1)
                                reqRowsFrame.release();
                            reqRowsMsg.close();
                            releaseRef();
                        } catch (Throwable t) {
                            log.error("Error while releasing resources for {}", this, t);
                        } finally {
                            serializer = Owned.safeRecycle(serializer, this);
                            bbView = null;
                        }
                    }
                } else if ((st&ST_CAN_RELEASE) != 0) {
                    if ((st&ST_QUERY_SENT) == 0 || (st&ST_GOT_TERM) != 0) {
                        st &= ~ST_SEND_QUERY;
                        var recycler = channelRecycler;
                        channelRecycler = ChannelRecycler.NOP;
                        journal("doRelease", this, ": recycle channel");
                        recycler.recycle(ctx.channel());
                    } else if ((st&(ST_QUERY_SENT|ST_CANCEL_SENT)) == ST_QUERY_SENT) {
                        st |= ST_SEND_CANCEL;
                        journal("doRelease", this, ": send cancel");
                        doSendCancel();
                    }
                } else {
                    journal("skip doRelease st=", st, ST, "on", this);
                }
            } finally { RELEASE_LOCK.setRelease(this, 0); }
        }

        /* --- --- --- event loop helpers --- --- --- */

        private static final BitsetRunnable.Spec BS_RUNNABLE_SPEC;
        public static final int AC_ALLOW_RELEASE;
        public static final int AC_SEND_CANCEL;
        public static final int AC_SEND_PING_ACK;
        public static final int AC_RESET;
        public static final int AC_RETRY;
        public static final int AC_SEND_QUERY;
        public static final int AC_SEND_BATCH;
        public static final int AC_SEND_BINDINGS_TERM;
        public static final int AC_SEND_REQ_ROWS;
        public static final int AC_RELEASE;
        public static final int AC_TOUCH_SINK;
        static {
            var s = new BitsetRunnable.Spec(MethodHandles.lookup());
            AC_ALLOW_RELEASE      = s.add("doAllowRelease",      false);
            AC_SEND_CANCEL        = s.add("doSendCancel",        true);
            AC_SEND_PING_ACK      = s.add("doSendPingAck",       false);
            AC_RESET              = s.add("doReset",             true);
            AC_RETRY              = s.add("doRetry",             false);
            AC_SEND_QUERY         = s.add("doSendQuery",         true);
            AC_SEND_BATCH         = s.add("doSendBatch",         false);
            AC_SEND_BINDINGS_TERM = s.add("doSendBindingsTerm",  true);
            AC_SEND_REQ_ROWS      = s.add("doSendReqRows",       false);
            AC_RELEASE            = s.add("doRelease",           false);
            AC_TOUCH_SINK         = s.setLast("doTouchSink");
            BS_RUNNABLE_SPEC = s;
        }

        private final class WsBitsetRunnable extends BitsetRunnable<WsHandler<?>> {
            public WsBitsetRunnable(WsHandler<?> receiver) {
                super(receiver, BS_RUNNABLE_SPEC);
            }

            @Override protected void onMethodError(String methodName, Throwable t) {
                super.onMethodError(methodName, t);
                if (ctx != null)
                    ctx.fireExceptionCaught(t);
                else if (parent.notTerminated())
                    clientSideError(t);
            }
        }

        private boolean inEventLoop() { return ctx == null || ctx.executor().inEventLoop(); }

        @SuppressWarnings("unused") private void doTouchSink() {
            if (ctx != null && bindType != null
                    && (st&(ST_BINDINGS_TERM_SENT|ST_SEND_BINDINGS_TERM)) == 0) {
                bbSink.touch();
            }
        }

        /* --- --- --- extension points --- --- --- */

        public void beforeParserCompletes() {
            assert inEventLoop() : "not in event loop";
            st |= ST_GOT_TERM;
        }

        /* --- --- --- message sending actions --- --- --- */

        public void sendQuery() { bsRunnable.sched(AC_SEND_QUERY); }

        @SuppressWarnings("unused") private void doSendQuery() {
            assert inEventLoop() : "not in event loop";
            if ((st&(ST_SEND_CANCEL|ST_CANCEL_SENT|ST_GOT_TERM|ST_CONNECTING)) != 0) {
                journal("skip doSendQuery st=", st, ST, "on", this);
                return;
            } else if (ctx == null) {
                st |= ST_CONNECTING;
                netty.open(this);
                return;
            } else if ((st&ST_QUERY_SENT) != 0) {
                throw new IllegalStateException("doSendQuery() while query is active");
            }
            var sparql = parent.sparql();
            int requiredBytes = sparql.len + 16;
            bbSink.sizeHint(Math.max(bbSink.sizeHint(), requiredBytes));
            bbSink.touch().ensureFreeCapacity(requiredBytes);
            bbSink.append(bindType == null ? QUERY_VERB : BIND_VERB[bindType.ordinal()]);
            bbSink.append(sparql);
            if (sparql.get(sparql.len-1) != '\n')
                bbSink.append('\n');
            var queryFrame = new TextWebSocketFrame(bbSink.take());
            st = (st&~ST_SEND_QUERY) | ST_QUERY_SENT;
            if (bindType != null) {
                parent.beforeSendBindQuery();
                serializer.init(bindingsVars, usefulBindingsVars, false);
                serializer.serializeHeader(bbSink.touch());
                ctx.write(queryFrame);
                ctx.write(new TextWebSocketFrame(bbSink.take())); // headers
                if (SEND_INFO) {
                    bbSink.touch().append(INFO).append(parent.journalName()).append('\n');
                    ctx.write(new TextWebSocketFrame(bbSink.take()));
                }
            } else {
                ctx.writeAndFlush(queryFrame);
            }
            bbSink.sizeHint(NORMAL_HINT);
            int actions = AC_SEND_REQ_ROWS;
            if ((st&ST_SEND_BATCH) != 0)
                actions |= AC_SEND_BATCH;
            if ((st&ST_SEND_BINDINGS_TERM) != 0)
                actions |= AC_SEND_BINDINGS_TERM;
            bsRunnable.sched(actions);
        }

        @Override public @Nullable Orphan<B> pollFillingBatch() {
            Orphan<B> tail;
            while ((int)BINDINGS_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                if ((tail=Batch.detachDistinctTail(receivedBindings)) != null)
                    journal("pollFillingBatch rows=", Batch.peekRows(tail), "on", this);
            } finally { BINDINGS_LOCK.setRelease(this, 0); }
            return tail;
        }

        protected void sendBatch(Orphan<B> b) {
            while ((int)BINDINGS_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                receivedBindings = quickAppend(receivedBindings, this, b);
            } finally { BINDINGS_LOCK.setRelease(this, 0); }
            bsRunnable.sched(AC_SEND_BATCH);
        }
        @SuppressWarnings("unused") private void doSendBatch() {
            assert inEventLoop() : "not in event loop";
            int subSt = st&(ST_QUERY_SENT|ST_SEND_CANCEL|ST_CANCEL_SENT|ST_BINDINGS_TERM_SENT);
            if (subSt == 0) {
                st |= ST_SEND_BATCH;
            } else if (ctx != null && subSt == ST_QUERY_SENT) {
                st &= ~ST_SEND_BATCH;
                while ((int)BINDINGS_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                    onSpinWait();
                B batch = receivedBindings;
                receivedBindings = null;
                BINDINGS_LOCK.setRelease(this, 0);
                if (batch != null) {
                    if (batch.rows > 0) {
                        serializer.serialize(batch.releaseOwnership(this),
                                             bbSink.touch(), REC_MAX_FRAME_LEN,
                                             parser, this);
                    } else {
                        Batch.recycle(batch, this);
                    }
                }
            } else  {
                journal("skip doSendBatch st=", st, ST, "on", this);
            }
            if (bsRunnable.isAnySched(AC_SEND_BINDINGS_TERM))
                bbSink.sizeHint(MIN_HINT);
        }

        @Override public void onSerializedChunk(ByteBuf chunk) {
            assert ctx != null;
            ctx.writeAndFlush(new TextWebSocketFrame(chunk));
        }

        public void sendBindingTerm(@Nullable Throwable cause) {
            if (cause != null && cause != CancelledException.INSTANCE)
                bindingsError = cause;
            bsRunnable.sched(AC_SEND_BINDINGS_TERM);
        }
        @SuppressWarnings("unused") private void doSendBindingsTerm() {
            assert inEventLoop() : "not in event loop";
            int subSt = st&(ST_QUERY_SENT|ST_SEND_CANCEL|ST_CANCEL_SENT|ST_BINDINGS_TERM_SENT|ST_GOT_TERM);
            if (subSt == 0) { // query not sent yet
                st |= ST_SEND_BINDINGS_TERM;
                journal("skip doSendBindingsTerm before query sent on", this);
            } else if (ctx != null && subSt == ST_QUERY_SENT) {
                // query sent, got no term, sent no term, cancel not queued
                bbSink.touch();
                if (bindingsError == null) {
                    bbSink.append(END_LF);
                } else {
                    bbSink.append(ERROR).append(' ');
                    bbSink.append(bindingsError.getClass().getSimpleName());
                    String msg = bindingsError.getMessage().replace("\n", "\\n");
                    bbSink.append(msg.substring(0, min(100, msg.length())));
                    bbSink.append('\n');
                }
                var bindTermFrame = new TextWebSocketFrame(bbSink.take());
                st = (st&~ST_SEND_BINDINGS_TERM) | ST_BINDINGS_TERM_SENT;
                ctx.writeAndFlush(bindTermFrame);
            } else {
                journal("skip doSendBindingsTerm st=", st, ST, "on", this);
            }
        }

        public void requestRows(long n) {
            if (n > 0) {
                REQ_ROWS.setRelease(this, n);
                bsRunnable.sched(AC_SEND_REQ_ROWS);
            }
        }
        @SuppressWarnings("unused") private void doSendReqRows() {
            assert inEventLoop() : "not in event loop";
            long n;
            var reqRowsFrame = this.reqRowsFrame;
            int subSt = st&(ST_QUERY_SENT|ST_SEND_CANCEL|ST_CANCEL_SENT|ST_GOT_TERM);
            if (ctx == null || subSt != ST_QUERY_SENT) {
                journal("skip doSendReqRows st=", st, ST, "on", this);
            } else if (reqRowsFrame.refCnt() != 1) {
                doSendReqRowsBadRefCnt();
            } else if ((n=(long)REQ_ROWS.getAndSetAcquire(this, 0)) <= 0) {
                journal("bogus doSendReqRows st=", st, ST, "on", this);
            } else {
                reqRowsMsg.len = REQUEST.length;
                if (n == Long.MAX_VALUE) reqRowsMsg.append(MAX_LF);
                else                     reqRowsMsg.append(n).append('\n');
                reqRowsFrame.content().readerIndex(0).writerIndex(reqRowsMsg.len);
                if (reqRowsMsg.u8() != reqRowsU8)
                    throw new IllegalStateException("reqRowsMsg grown");
                ctx.writeAndFlush(reqRowsFrame.retain());
            }
        }
        private void doSendReqRowsBadRefCnt() {
            assert inEventLoop() : "not in event loop";
            if (reqRowsFrame.refCnt() < 1) {
                String state = parent.renderState();
                journal("reqRowsFrame released, st=", state, "on", this);
                throw new IllegalStateException("reqRowsFrame released, st="+state+" on "+this);
            } else {
                journal("doSendRequestRows(): frame in-use on", this);
                if (ctx != null)
                    ctx.executor().execute(() -> bsRunnable.sched(AC_SEND_REQ_ROWS));
            }
        }

        public void sendCancel() {
            if (ctx != null)
                bsRunnable.sched(AC_SEND_CANCEL);
        }
        private void doSendCancel() {
            assert inEventLoop() : "not in event loop";
            if (ctx != null && (st&(ST_QUERY_SENT|ST_CANCEL_SENT|ST_GOT_TERM)) == ST_QUERY_SENT) {
                var cancel = new TextWebSocketFrame(bbSink.touch().append(CANCEL_LF).take());
                st  = (st&~ST_SEND_CANCEL) | ST_CANCEL_SENT;
                ctx.writeAndFlush(cancel);
            } else {
                journal("skip doSendCancel st=", st, ST, "on", this);
            }
        }

        public void sendPingAck() { bsRunnable.sched(AC_SEND_PING_ACK); }
        @SuppressWarnings("unused") private void doSendPingAck() {
            if (ctx != null && ctx.channel().isActive())
                ctx.writeAndFlush(new TextWebSocketFrame(bbSink.touch().append(PING_ACK).take()));
        }

        /* --- --- --- NettyWsClientHandler --- --- --- */

        @Override public void attach(ChannelHandlerContext ctx, ChannelRecycler recycler) {
            assert this.ctx == null : "previous attach()";
            bsRunnable.executor(ctx.executor());
            this.ctx = ctx;
            this.channelRecycler = recycler;
            assert inEventLoop() : "not in event loop";
            st = (st&~ST_CONNECTING) | ST_ATTACHED | ((st&ST_SEND_CANCEL) == 0 ? ST_SEND_QUERY : 0);
            parent.setChannel(ctx.channel());
            bbSink.alloc(ctx.alloc());
            bsRunnable.sched(AC_SEND_QUERY);
        }

        private void clientSideError(@Nullable Throwable error) {
            if (parent.isTerminated()) {
                journal("parser terminated, ignoring clientSideError", error, "on", this);
                journal("st=", st, ST, "on", this);
            } else {
                FSException ex;
                if (error instanceof FSServerException se && (st&ST_GOT_FRAMES) != 0) {
                    ex = se.shouldRetry(false);
                } else if (error == null) {
                    boolean empty = (st & ST_GOT_FRAMES) == 0;
                    if (empty && retries < FSNettyProperties.maxRetries()) {
                        journal("will retry query st=", st, ST, "on", this);
                        ++retries;
                        st |= ST_RETRY;
                        return;
                    } else {
                        String msg = empty
                                ? "server closed WebSocket before !end/!error/!cancelled"
                                : "incomplete response";
                        ex = new InvalidSparqlResultsException(msg).shouldRetry(empty);
                    }
                } else {
                    ex = FSException.wrap(endpoint, error);
                }
                ex.endpoint(endpoint);
                parser.feedError(ex);
            }
        }

        @Override public void detach(@Nullable Throwable error) {
            assert inEventLoop() : "no in event loop";
            bsRunnable.runNow();
            channelRecycler = ChannelRecycler.CLOSE;
            parent.setChannel(null);
            if (parent.notTerminated())
                clientSideError(error);
            if (parser.noServerTermination() && ctx != null)
                ctx.close();
            ctx = null;
            bsRunnable.executor(ForkJoinPool.commonPool());
            st = (st&~(ST_ATTACHED|ST_CONNECTING)) | ST_GOT_TERM;
            int actions = (st&ST_RETRY) != 0 ? AC_RETRY
                        : ((st&ST_CAN_RELEASE) != 0 ? AC_RELEASE : 0);
            if (actions != 0)
                bsRunnable.sched(actions);
        }

        @Override public void frame(WebSocketFrame frame) {
            assert inEventLoop() : "not in event loop";
            bsRunnable.runNow();
            if (frame instanceof CloseWebSocketFrame) {
                if (parent.notTerminated())
                    clientSideError(null);
            } else if (!(frame instanceof TextWebSocketFrame f)) {
                String msg = "Unexpected frame type: "+frame.getClass().getSimpleName();
                throw new FSServerException(msg).shouldRetry(false);
            } else if ((st&ST_RELEASED) != 0) {
                throw new IllegalStateException(this+" released, cannot handle frame, st="+parent.renderState());
            } else {
                st |= ST_GOT_FRAMES;
                try {
                    parser.feedShared(bbView.wrapAsSingle(f.content()));
                } catch (BatchQueue.QueueStateException e) { doSendCancel(); }
            }
        }

        /* --- --- --- ChannelBound --- --- -- */

        @Override public @Nullable Channel channelOrLast() {return parent.channelOrLast();}
        @Override public void setChannel(Channel ch) {throw new UnsupportedOperationException();}
        @Override public String journalName() {
            Channel ch = parent.channelOrLast();
            return "C.WH:" + (ch == null ? "null" : ch.id().asShortText())
                          + '@' + parent.instanceId();
        }

        @Override public String toString() {
            var sb = new StringBuilder().append(journalName()).append(ST.render(st));
            if (info != null)
                sb.append("{info=").append(info).append('}');
            return sb.toString();
        }
    }

    private final class WsEmitter<B extends Batch<B>> extends CallbackEmitter<B, WsEmitter<B>>
            implements WsStreamNode<B>, Orphan<WsEmitter<B>> {
        private final SparqlQuery query;
        private final WsHandler<B> h;
        private final @Nullable EmitBindQuery<B> bindQuery;
        private @Nullable Binding binding;
        private final Vars bindableVars;
        private final Emitter<B, ?> leftEmitter;
        private @Nullable Channel lastChannel;

        public WsEmitter(BatchType<B> batchType, Vars outVars, SparqlQuery query,
                         @Nullable EmitBindQuery<B> bindQuery) {
            super(batchType, outVars, EMITTER_SVC, RR_WORKER, CREATED, CB_FLAGS);
            this.h         = new WsHandler<>(new EmitWsParser(bindQuery), this, bindQuery);
            this.query     = query;
            this.bindQuery = bindQuery;
            if (bindQuery == null) {
                bindableVars = query.allVars();
                leftEmitter = null;
            } else  {
                bindableVars = bindQuery.bindingsVars().union(query.allVars());
                leftEmitter = bindQuery.bindings.takeOwnership(this);
                leftEmitter.subscribe(new BindingsReceiver());
            }
        }

        @Override public WsEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}

        @Override protected void doRelease() {
            super.doRelease();
            Owned.safeRecycle(leftEmitter, this);
            h.release();
        }

        /* --- --- --- rebind --- --- --- */

        @Override public void rebind(BatchBinding binding) throws RebindException {
            int st = resetForRebind(CLEAR_ON_REBIND, LOCKED_MASK);
            try {
                assert binding.batch != null && binding.row < binding.batch.rows : "bad binding";
                this.binding = binding;
                if (leftEmitter != null)
                    leftEmitter.rebind(binding);
                h.reset();
            } finally {
                unlock(st);
            }
        }

        @Override public Vars bindableVars() { return bindableVars; }

        /* --- --- --- StreamNode --- --- --- */

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return bindQuery == null ? Stream.empty() : Stream.of(leftEmitter);
        }

        /* --- --- --- ChannelBound --- --- --- */

        @Override public @Nullable Channel channelOrLast() { return lastChannel; }
        @Override public void setChannel(Channel ch)       { if (ch != null) lastChannel = ch; }

        @Override public String journalName() {
            return "C.WE:" + (lastChannel == null ? "null" : lastChannel.id().asShortText())
                           + '@' + Integer.toHexString(System.identityHashCode(this));
        }

        @Override protected StringBuilder minimalLabel() {
            return new StringBuilder().append(journalName());
        }

        @Override protected void appendToSimpleLabel(StringBuilder out) {
            super.appendToSimpleLabel(out);
            if (h.info != null) out.append(" info=").append(h.info);
        }
        /* --- --- --- CallbackEmitter --- --- --- */

        @Override protected void        startProducer() { h.sendQuery(); }
        @Override protected void        pauseProducer() {}
        @Override protected void resumeProducer(long n) { h.requestRows(n); }
        @Override protected void       cancelProducer() { h.sendCancel(); }
        @Override protected void  earlyCancelProducer() {}
        @Override protected void      releaseProducer() { h.release(); }

        /* --- --- --- WsStreamNode --- --- --- */

        @Override public SegmentRope sparql() {
            return (binding == null ? query : query.bound(binding)).sparql();
        }

        @Override public void beforeSendBindQuery() {
            long requested = requested();
            assert bindQuery != null;
            Emitter<B, ?> bindings = leftEmitter;
            if (requested > 0) {
                int chunk = min(bindings.preferredRequestChunk(),
                                preferredRequestChunk());
                bindings.request(min(requested, chunk>>2));
            }
        }

        @Override public String renderState() {return flags.render(state());}

        @Override public String instanceId() {
            return Integer.toHexString(System.identityHashCode(this));
        }

        /* --- --- --- parser --- --- --- */

        private final class EmitWsParser extends WsParser<B> {
            public EmitWsParser(EmitBindQuery<B> bindQuery) { super(WsEmitter.this, bindQuery); }

            @Override protected void handleBindRequest(long n) {
                if (leftEmitter == null)
                    throw new UnsupportedOperationException();
                leftEmitter.request(n);
            }
            @Override protected void onPing() {h.sendPingAck();}
        }

        /* --- --- --- bindings Receiver --- --- --- */

        private final class BindingsReceiver implements Receiver<B>, HasFillingBatch<B> {
            @Override public Stream<? extends StreamNode> upstreamNodes() {
                return Stream.ofNullable(leftEmitter);
            }
            @Override public String label(StreamNodeDOT.Label type) {
                return WsEmitter.this.label(type);
            }
            @Override public String journalName() {
                return WsEmitter.this.journalName()+".B";
            }
            @Override public void onBatch(Orphan<B> orphan) {
                if (Batch.peekRows(orphan) == 0)
                    Orphan.recycle(orphan);
                else
                    h.sendBatch(orphan);
            }
            @Override public void onBatchByCopy(B batch) {
                if (batch == null || batch.rows == 0)
                    return;
                Orphan<B> orphan = h.pollFillingBatch();
                if (orphan == null)
                    orphan = bt.create(batch.cols);
                B f = orphan.takeOwnership(this);
                f.copy(batch);
                h.sendBatch(f.releaseOwnership(this));
            }
            @Override public @Nullable Orphan<B> pollFillingBatch() {
                return h.pollFillingBatch();
            }
            @Override public void  onComplete()            { h.sendBindingTerm(null); }
            @Override public void onCancelled()            { h.sendBindingTerm(CancelledException.INSTANCE); }
            @Override public void onError(Throwable cause) { h.sendBindingTerm(cause); }
        }

    }
}
