package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.FlowModel;
import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.ItBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.netty.util.*;
import com.github.alexishuf.fastersparql.emit.*;
import com.github.alexishuf.fastersparql.emit.async.BItEmitter;
import com.github.alexishuf.fastersparql.emit.async.CallbackEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.ContentNegotiator;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.*;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.sparql.results.AbstractWsParser;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import com.github.alexishuf.fastersparql.sparql.results.WsBindingSeq;
import com.github.alexishuf.fastersparql.sparql.results.WsServerParser;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.*;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.ReferenceCounted;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties.sharedEventLoopGroupKeepAliveSeconds;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope.asFinal;
import static com.github.alexishuf.fastersparql.sparql.results.AbstractWsParser.REC_MAX_FRAME_LEN;
import static com.github.alexishuf.fastersparql.util.UriUtils.unescape;
import static com.github.alexishuf.fastersparql.util.UriUtils.unescapeToRope;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.render;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED;
import static io.netty.handler.codec.http.HttpHeaderValues.CHUNKED;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.util.AsciiString.indexOfIgnoreCaseAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NettySparqlServer implements AutoCloseable{
    private static final Logger log = LoggerFactory.getLogger(NettySparqlServer.class);
    private static final EventLoopGroupHolder ACCEPT_ELG
            = new EventLoopGroupHolder("NettySparqlServer.ACCEPT_ELG", null,
                                       sharedEventLoopGroupKeepAliveSeconds(), SECONDS, 1);
    private static final int HANDLER_POOL_SIZE = FSNettyProperties.serverHandlerPool();

    private static final String SP_PATH = "/sparql";
    private static final String APPLICATION_SPARQL_QUERY = "application/sparql-query";
    private static final String TEXT_PLAIN_U8 = "text/plain; charset=utf-8";
    private static final boolean SEND_INFO = FSNettyProperties.channelInfo();

    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup workerGroup;
    private final Channel server;
    private final SparqlClient sparqlClient;
    private final LIFOPool<SparqlHandler> sparqlHandlerPool
            = new LIFOPool<>(SparqlHandler.class, null, HANDLER_POOL_SIZE, 4096);
    private final LIFOPool<WsHandler> wsHandlerPool
            = new LIFOPool<>(WsHandler.class, null, HANDLER_POOL_SIZE, 4096);
    private final boolean useBIt;
    private final ContentNegotiator negotiator = ResultsSerializer.contentNegotiator();
    private final SparqlClient.@Nullable Guard sparqlClientGuard;
    private final FastAliveSet<QueryHandler<?>> queryHandlers = new FastAliveSet<>(512);
    private final @Nullable EventLoopGroupHolder workerELGHolder;
    private @MonotonicNonNull Semaphore handlersClosed;
    private @MonotonicNonNull FSCancelledException serverClosing;

    public NettySparqlServer(FlowModel flowModel, SparqlClient client, boolean sharedSparqlClient,
                             String host, int port) {
        this.sparqlClient = client;
        this.sparqlClientGuard = sharedSparqlClient ? sparqlClient.retain() : null;
        this.useBIt = switch (flowModel) {
            case ITERATE -> true;
            case EMIT -> false;
        };
        if (FSNettyProperties.shareEventLoopGroup()) {
            acceptGroup     = ACCEPT_ELG.acquire();
            workerELGHolder = SharedEventLoopGroupHolder.get();
            workerGroup     = workerELGHolder.acquire();
        } else {
            workerELGHolder = null;
            acceptGroup     = new NioEventLoopGroup(1);
            workerGroup     = new NioEventLoopGroup(FSProperties.nettyEventLoopThreads());
        }
        server = new ServerBootstrap().group(acceptGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    private final String debugName = FSNettyProperties.debugServerChannel()
                            ? NettySparqlServer.this.sparqlClient.toString() : null;
                    @Override protected void initChannel(SocketChannel ch) {
                        var sparql = sparqlHandlerPool.get();
                        var ws     = wsHandlerPool.get();

                        ChannelPipeline p = ch.pipeline();
                        p.addLast("http", new HttpServerCodec());
                        if (debugName != null)
                            p.addLast("debug", new NettyChannelDebugger(debugName));
                        //p.addLast("log", new LoggingHandler(NettySparqlServer.class, LogLevel.INFO, ByteBufFormat.HEX_DUMP));
                        p.addLast("req-aggregator", new HttpObjectAggregator(1<<15, true));
                        p.addLast("keepalive", new io.netty.handler.codec.http.HttpServerKeepAliveHandler());
                        //p.addLast("ws-compression", new WebSocketServerCompressionHandler());
                        p.addLast("sparql", sparql == null ? new SparqlHandler() : sparql);
                        p.addLast("ws", new WebSocketServerProtocolHandler(SP_PATH, null, true));
                        p.addLast("ws-sparql", ws == null ? new WsHandler() : ws);
                    }
                }).bind(host, port).syncUninterruptibly().channel();
    }

    public int                        port() { return listenAddress().getPort(); }
    public InetSocketAddress listenAddress() { return (InetSocketAddress)server.localAddress(); }

    @Override public String toString() {
        return "NettySparqlServer"+server+"("+sparqlClient.endpoint()+")";
    }

    @Override public void close() {
        if (serverClosing != null) return;
        serverClosing = new FSCancelledException(null, "cancelled by server.close()");
        int[] handlersCount = {0};
        handlersClosed = new Semaphore(0);
        if (!server.close().awaitUninterruptibly(2, SECONDS))
            log.error("Server close timeout on {}", this);
        queryHandlers.destruct(h -> {
            try {
                ++handlersCount[0];
                h.close();
            } catch (Throwable t) {
                log.info("Ignoring {} while closing {} due to shutdown of server at {}",
                        t.getClass().getSimpleName(), h, this);
                handlersClosed.release();
            }
        });
        try {
            if (!handlersClosed.tryAcquire(handlersCount[0], 10, SECONDS)) {
                int done = handlersClosed.availablePermits();
                log.warn("{} handlers closed, abandoning {} while closing {} due to timeout",
                        done, handlersCount[0] - done, this);
            }
        } catch (InterruptedException ignored) {}
        if (workerELGHolder != null) {
            ACCEPT_ELG.release();
            workerELGHolder.release();
        } else {
            if (!acceptGroup.shutdownGracefully(50, 100, MILLISECONDS)
                            .awaitUninterruptibly(1, SECONDS)) {
                log.warn("acceptGroup too slow to to shutdown, leaking from {}", this);
            }
            if (workerGroup.shutdownGracefully(50, 100, SECONDS)
                           .awaitUninterruptibly(1, SECONDS)) {
                log.warn("workerGroup too slow to to shutdown, leaking from {}", this);
            }
        }
        if (sparqlClientGuard == null) sparqlClient.close();
        else                           sparqlClientGuard.close();
    }

    @ChannelHandler.Sharable
    private abstract class QueryHandler<I> extends SimpleChannelInboundHandler<I>
            implements ChannelBound, Requestable,
                       Receiver<CompressedBatch>, HasFillingBatch<CompressedBatch>,
                       ResultsSerializer.ChunkConsumer<ByteBuf> {
        protected static final int AC_SEND_BATCH;
        protected static final int AC_BIND_REQ;
        protected static final int AC_SEND_TERM;
        protected static final int AC_CLOSE;
        protected static final int AC_RECYCLE;
        protected static final int AC_TOUCH_SINK;
        private static final BitsetRunnable.Spec BS_RUNNABLE_SPEC;
        protected static final VarHandle Q;
        static {
            try {
                Q = MethodHandles.lookup().findVarHandle(QueryHandler.class, "plainQueueLock", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
            var s = new BitsetRunnable.Spec(MethodHandles.lookup());
            BS_RUNNABLE_SPEC = s;
            AC_SEND_BATCH = s.add("doSendBatch",       false);
            AC_BIND_REQ   = s.add("doBindReq",         false);
            AC_SEND_TERM  = s.add("doSendTerm",        false);
            AC_CLOSE      = s.add("doClose",           false);
            AC_RECYCLE    = s.add("doReleaseOrRecycle", true);
            AC_TOUCH_SINK = s.setLast("doTouchSink");
        }

        protected static final int ST_GOT_REQ               = 0x001;
        protected static final int ST_RES_STARTED           = 0x002;
        protected static final int ST_RES_TERMINATED        = 0x004;
        protected static final int ST_CLOSING               = 0x010;
        protected static final int ST_NOTIFIED_CLOSED       = 0x020;
        protected static final int ST_CANCEL_REQ            = 0x040;
        protected static final int ST_DIRECT_RES_TERMINATED = 0x080;
        protected static final int ST_ORPHAN                = 0x100;
        protected static final int ST_RELEASED              = 0x200;
        protected static final int ST_UNHEALTHY             = 0x400;

        protected static final LongRenderer ST = st -> {
            if (st == 0) return "[]";
            var sb = new StringBuilder().append('[');
            if ((st&ST_GOT_REQ)               != 0) sb.append("GOT_REQ,");
            if ((st&ST_RES_STARTED)           != 0) sb.append("RES_STARTED,");
            if ((st&ST_RES_TERMINATED)        != 0) sb.append("RES_TERMINATED,");
            if ((st&ST_CLOSING)               != 0) sb.append("CLOSING,");
            if ((st&ST_NOTIFIED_CLOSED)       != 0) sb.append("NOTIFIED_CLOSE,");
            if ((st&ST_CANCEL_REQ)            != 0) sb.append("CANCEL_REQ,");
            if ((st&ST_DIRECT_RES_TERMINATED) != 0) sb.append("ST_DIRECT_RES_TERMINATED,");
            if ((st&ST_ORPHAN)                != 0) sb.append("ORPHAN,");
            if ((st&ST_RELEASED)              != 0) sb.append("RELEASED,");
            if ((st&ST_UNHEALTHY)             != 0) sb.append("UNHEALTHY,");
            sb.setLength(sb.length()-1);
            return sb.append(']').toString();
        };

        protected static final ResultsSerializer.Recycler<CompressedBatch> BATCH_RECYCLER
                = ResultsSerializer.recycler(COMPRESSED);

        protected SparqlParser parser = SparqlParser.create().takeOwnership(this);
        protected ByteBufRopeView bbView = new ByteBufRopeView(new SegmentRopeView());
        protected @MonotonicNonNull ByteBufSink bbSink = new ByteBufSink(ByteBufAllocator.DEFAULT);
        protected @MonotonicNonNull ChannelHandlerContext ctx;
        @SuppressWarnings("unused") private int plainQueueLock;
        private @Nullable Emitter<CompressedBatch, ?> upstream;
        private @Nullable Throwable errorOrCancelledException;
        private short smallBatchRows;
        private short cols;
        protected CompressedBatch sendQueue;
        protected int st;
        private @MonotonicNonNull MutableRope queryRope;
        protected HandlerBitsetRunnable bsRunnable = new HandlerBitsetRunnable();
        protected @Nullable String info;
        private Channel lastCh;
//        protected Watchdog.Spec watchdogSpec;
//        protected Watchdog watchdog;
//
//        protected void makeWatchdogSpec(Rope sparql) {
//            watchdogSpec = Watchdog.spec(journalName()).sparql(sparql.toString()).noThread()
//                                   .streamNode(this);
//        }

        public QueryHandler() {
            st = ST_ORPHAN;
            queryHandlers.add(this);
            bbSink.sizeHint(ByteBufSink.MIN_HINT);
        }

        protected final MutableRope queryRope(int capacity) {
            var queryRope = this.queryRope;
            if (queryRope == null)
                this.queryRope = queryRope = new MutableRope(capacity);
            else
                queryRope.len = 0;
            return queryRope;
        }

        /* --- --- --- NettyEmitSparqlServer.close() --- --- --- */

        public void close() {
            bsRunnable.sched(AC_CLOSE);
        }

        @SuppressWarnings("unused") private void doClose() {
            st |= ST_CLOSING;
            if (upstream != null) {
                journal("doClose, upstream.cancel()", this);
                if (errorOrCancelledException != null)
                    errorOrCancelledException = serverClosing;
                boolean did = upstream.cancel();
            } else if (ctx != null && ctx.channel().isActive()) {
                var otherCls = this instanceof SparqlHandler
                        ? WsHandler.class : SparqlHandler.class;
                QueryHandler<?> other = ctx.pipeline().get(otherCls);
                boolean hasQuery = other.upstream != null;
                if (hasQuery) {
                    journal("doClose, skip ctx.close(), other handler has upstream", this);
                } else {
                    journal("doClose, ctx.close9)", this);
                    ctx.close();
                }
            } else {
                notifyClosed();
            }
        }
        private void notifyClosed() {
            if ((st&ST_NOTIFIED_CLOSED) != 0)
                return;
            shouldBeInEventLoop("notifyClosed");
            journal("notifyClosed", this);
            st |= ST_NOTIFIED_CLOSED;
            var handlersClosed = NettySparqlServer.this.handlersClosed;
            if (handlersClosed != null)
                handlersClosed.release();
        }

        /* --- --- --- Receiver --- --- --- */

        protected final boolean cancel() {
            shouldBeInEventLoop("cancel");
            if (upstream != null) {
                st |= ST_CANCEL_REQ;
                upstream.cancel();
                return true;
            }
            return false;
        }

        public final void request(long n) {
            shouldBeInEventLoop("request");
            if (upstream != null && (st&ST_CANCEL_REQ) == 0) {
                upstream.request(n);
            } else {
                journal("skip request", n, "st=", st, ST, "on", this);
                if ((st&(ST_RES_STARTED|ST_RES_TERMINATED)) == 0
                        || (st&(ST_ORPHAN |ST_RELEASED|ST_CANCEL_REQ)) != 0) {
                    log.warn("Ignoring unexpected request {} on {}", n, this,
                             new Exception("request()ed here"));
                }
            }
        }

        protected Emitter<CompressedBatch, ?>
        subscribeTo(Orphan<? extends Emitter<CompressedBatch, ?>> orphan) {
            while ((int)Q.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
            Vars emVars;
            var em = orphan.takeOwnership(this);
            try {
                emVars = em.vars();
                shouldBeInEventLoop("subscribeTo");
                checkReleasedOrOrphan("subscribeTo", em);
                if (upstream != null)
                    onSuspicious("subscribeTo, already has upstream");
                if ((st&ST_CLOSING) != 0)
                    onSuspicious("subscribeTo with ST_CLOSING");
                if ((st&ST_CANCEL_REQ) != 0) {
                    onSuspicious("subscribeTo with ST_CANCEL_REQ");
                    st &= ~ST_CANCEL_REQ;
                }
                if ((st&(ST_RES_STARTED|ST_RES_TERMINATED)) != ST_RES_STARTED)
                    throw new IllegalStateException("subscribeTo: response not started");
                int emCols = emVars.size();
                if (emCols > Short.MAX_VALUE)
                    throw new IllegalStateException("Too many upstream columns");
                errorOrCancelledException = null;
                sendQueue                 = Batch.safeRecycle(sendQueue, this);
                upstream                  = em;
                cols                      = (short)emCols;
                //watchdog                = watchdogSpec.startSecs(10);
            } catch (Throwable t) {
                Emitters.discard(em.releaseOwnership(this));
                throw t;
            } finally {
                Q.setRelease(this, 0);
            }
            // a batch using < 6.25% of its rows capacity is small. See doSendBatch()
            smallBatchRows = (short)Math.min(Short.MAX_VALUE,
                                             COMPRESSED.preferredRowsPerBatch(emVars)>>4);
            em.subscribe(this);
            bbSink.sizeHint(ByteBufSink.NORMAL_HINT);
            bsRunnable.sched(AC_TOUCH_SINK);
            return em;
        }

        @Override public Orphan<CompressedBatch> pollFillingBatch() {
            Orphan<CompressedBatch> tail;
            while ((int)Q.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
            try {
                tail = Batch.detachDistinctTail(sendQueue);
            } finally { Q.setRelease(this, 0); }
            return tail;
        }

        @Override public void onBatchByCopy(CompressedBatch batch) {
            if (batch == null || batch.rows == 0) return;
            var orphan = pollFillingBatch();
            if (orphan == null)
                orphan = COMPRESSED.create(cols);
            var f = orphan.takeOwnership(this);
            f.copy(batch);
            orphan = f.releaseOwnership(this);
            while ((int)Q.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
            try {
                sendQueue = Batch.quickAppend(sendQueue, this, orphan);
            } finally { Q.setRelease(this, 0); }
            bsRunnable.sched(AC_SEND_BATCH);
        }
        @Override public void onBatch(Orphan<CompressedBatch> orphan) {
            if (orphan == null)
                return;
            while ((int)Q.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
            try {
                sendQueue = Batch.quickAppend(sendQueue, this, orphan);
            } finally { Q.setRelease(this, 0); }
            bsRunnable.sched(AC_SEND_BATCH);
        }

        @Override public void onComplete() {
            assert upstream != null : "suspicious onComplete with upstream=null";
            bsRunnable.sched(AC_SEND_TERM);
        }

        @Override public void onCancelled() {
            assert upstream != null : "suspicious onCancelled with upstream=null";
            if (errorOrCancelledException == null)
                errorOrCancelledException = CancelledException.INSTANCE;
            bsRunnable.sched(AC_SEND_TERM);
        }

        @Override public void onError(Throwable cause) {
            assert upstream != null : "suspicious onError with upstream=null";
            if (errorOrCancelledException == null)
                errorOrCancelledException = cause;
            bsRunnable.sched(AC_SEND_TERM);
        }

        /* --- --- --- event-loop side of Receiver --- --- --- */

        protected abstract void serialize(CompressedBatch batch);

        @SuppressWarnings("unused") private void doSendBatch() {
            boolean canSend = (st&CAN_SEND_BATCH) == ST_RES_STARTED, detach;
            CompressedBatch b;
            while ((int)Q.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
            try {
                b         = sendQueue;
                detach    = canSend && b != null && b.rows > smallBatchRows;
                sendQueue = detach ? Orphan.takeOwnership(b.detachHead(), this) : null;
            } finally { Q.setRelease(this, 0); }
            if (b != null) {
                if (canSend) {
                    serialize(b);
                    if (detach)
                        bsRunnable.sched(AC_SEND_BATCH);
                } else {
                    journal("skip&recycle doSendBatch, st=", st, ST, "on", this);
                    Batch.recycle(b, this);
                }
            }
            if (bsRunnable.isAnySched(AC_SEND_TERM))
                bbSink.sizeHint(ByteBufSink.MIN_HINT);
        }
        private static final int CAN_SEND_BATCH = ST_RES_STARTED | ST_RES_TERMINATED
                                                | ST_DIRECT_RES_TERMINATED | ST_CANCEL_REQ
                                                | ST_ORPHAN | ST_RELEASED;

        @SuppressWarnings("unused") private void doSendTerm() {
            upstream = Owned.recycle(upstream, this);
            st &= ~ST_CANCEL_REQ;
            try {
                if ((st&CAN_SEND_TERM) == ST_GOT_REQ) {
                    endResponse(termMessage(errorOrCancelledException));
                } else {
                    journal("skip doSendTerm st=", st, ST, "on", this);
                    if (errorOrCancelledException != null)
                        journal("skip doSendTerm cause=", errorOrCancelledException, "on", this);
                }
            } finally {
                if ((st&ST_CLOSING) != 0)
                    notifyClosed();
            }
        }
        private static final int CAN_SEND_TERM = ST_GOT_REQ | ST_RES_TERMINATED
                                               | ST_DIRECT_RES_TERMINATED
                                               | ST_ORPHAN | ST_RELEASED;

        @SuppressWarnings("unused") protected void doBindReq() { throw new UnsupportedOperationException(); }

        /* --- --- --- response start/termination management --- --- --- */

        /**
         * Build a {@link FullHttpMessage}/{@link LastHttpContent}/{@link TextWebSocketFrame}
         * that symbolizes the end of the response and report {@code cause} to the client,
         * if non-null.
         *
         * <p>This will be called from the event loop, the following will hold about {@code st}:</p>
         *
         * <ul>
         *     <li>Both {@link #ST_RELEASED} and {@link #ST_ORPHAN} are unset</li>
         *     <li>{@link #ST_GOT_REQ} is set</li>
         *     <li>{@link #ST_RES_TERMINATED} is not set</li>
         *     <li>{@link #ST_RES_STARTED} <strong>MAY</strong> be set</li>
         * </ul>
         */
        protected abstract Object termMessage(@Nullable Throwable cause);

        /** Used by {@link #channelUnregistered(ChannelHandlerContext)} to recycle the handler */
        protected abstract LIFOPool<? extends QueryHandler<I>> pool();

        protected void onNewRequest() {
            checkReleasedOrOrphan("onNewRequest", null);
            if ((st&ST_GOT_REQ) != 0) {
                throw new IllegalStateException("client sent a new request before receiving " +
                                                "a response to its previous request");
            } if (serverClosing != null) {
                throw serverClosing;
            }
            shouldBeInEventLoop("onNewRequest");
            st = st&~(ST_RES_STARTED|ST_RES_TERMINATED) | ST_GOT_REQ;
        }
        protected void startResponse() {
            checkReleasedOrOrphan("startResponse", null);
            startResponse0();
        }
        private void startResponse0() {
            if ((st&ST_GOT_REQ) == 0)
                throw new IllegalStateException("startResponse without request");
            if ((st&ST_RES_STARTED) != 0)
                throw new IllegalStateException("startResponse: already started");
            shouldBeInEventLoop("startResponse");
            st |= ST_RES_STARTED;
        }
        protected void startResponse(Object msg, boolean flush) {
            checkReleasedOrOrphan("startResponse", msg);
            startResponse0();
            if (flush) ctx.writeAndFlush(msg);
            else       ctx.write(msg);
        }

        protected void endResponse(Object msg) {
            try {
                checkReleasedOrOrphan("endResponse", msg);
                if ((st&ST_RES_TERMINATED) != 0)
                    throw new IllegalStateException("response already terminated");
                shouldBeInEventLoop("endResponse");
                st = (st&~ST_GOT_REQ) | ST_RES_TERMINATED;
                ctx.writeAndFlush(msg);
                //if (watchdog != null) watchdog.stop();
            } finally {
                try {
                    cleanupAfterEndResponse();
                } catch (Throwable t) {
                    log.error("ignoring {} from cleanupAfterEndResponse on {}",
                              t.getClass().getSimpleName(), this, t);
                }
                if ((st&ST_CLOSING) != 0)
                    doClose();
            }
        }

        protected void cleanupAfterEndResponse() {
            if (!cancel())
                st &= ~ST_CANCEL_REQ;
            bbSink.close();
            while((int)Q.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
            try {
                sendQueue = Batch.recycle(sendQueue, this);
            } finally { Q.setRelease(this, 0); }
            errorOrCancelledException = null;
//            watchdogSpec = null;
//            if (watchdog != null)
//                watchdog.stop();
        }

        protected void send(Object msg) {
            checkReleasedOrOrphan("send", msg);
            if ((st&(ST_RES_STARTED|ST_RES_TERMINATED)) != ST_RES_STARTED)
                throw new IllegalStateException("response terminated or not started");
            shouldBeInEventLoop("send");
            ctx.writeAndFlush(msg);
        }

        /* --- --- --- run in event loop helpers --- --- --- */

        private final class HandlerBitsetRunnable extends BitsetRunnable<QueryHandler<I>> {
            public HandlerBitsetRunnable() {super(QueryHandler.this, BS_RUNNABLE_SPEC);}

            @Override protected void onMethodError(String methodName, Throwable t) {
                super.onMethodError(methodName, t);
                exceptionCaught(ctx, t);
            }
        }

        private boolean inEventLoop() {
            return ctx == null ? bsRunnable.inRun() : ctx.executor().inEventLoop();
        }

        protected void shouldBeInEventLoop(String action) {
            if (!inEventLoop())
                onSuspicious(action+" outside event loop");
        }

        protected void onSuspicious(String msg) {
            journal("st=", st, ST, msg, "on", this);
            assert false : msg;
        }

        @SuppressWarnings("unused") private void doTouchSink() {
            if ((st&CANT_TOUCH_SINK) == 0 && ctx.channel().isActive())
                bbSink.touch();
        }
        private static final int CANT_TOUCH_SINK = ST_ORPHAN | ST_RELEASED | ST_UNHEALTHY
                                                 | ST_CLOSING | ST_NOTIFIED_CLOSED;

        /* --- --- --- ChannelBound --- --- --- */

        @Override public @Nullable Channel channelOrLast() {return lastCh;}

        protected String idTag() {
            return lastCh == null ? '@'+Integer.toHexString(System.identityHashCode(this))
                                  : ':'+lastCh.id().asShortText();
        }

        @Override public void setChannel(Channel ch) {
            if (ch != channelOrLast())
                throw new UnsupportedOperationException();
        }

        @Override public String journalName() {
            return "S."+getClass().getSimpleName().charAt(0)+idTag();
        }

        @Override public String toString() {
            var sb = new StringBuilder().append(journalName()).append(ST.render(st));
            if (info != null) sb.append("{info=").append(info).append('}');
            return sb.toString();
        }

        /* --- --- --- StreamNode --- --- --- */

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.ofNullable(upstream);
        }

        @Override public String label(StreamNodeDOT.Label type) {
            String name = journalName();
            if (type.showState()) {
                var sb = new StringBuilder().append(name).append(ST.render(st));
                if (info != null)
                    sb.append(" info=").append(info);
                int sendQueueRows, witness = (int)Q.compareAndExchangeAcquire(this, 0, 1);
                try {
                    sendQueueRows = sendQueue == null ? 0 : sendQueue.totalRows();
                } finally { if (witness == 0) Q.setRelease(this, 0); }
                sb.append(" sendQueue=").append(sendQueueRows);
                //sb.append(" batchesSerialized=").append(batchesSerialized);
                //sb.append(" rowsSerialized=").append(rowsSerialized);
                return sb.toString();
            }
            return name;
        }

        /* --- --- --- handler lifecycle (ST_ORPHAN/ST_RELEASED) --- --- --- */

        protected void releaseResources() {
            if ((st&ST_RELEASED) != 0)
                journal("duplicate releaseResources st=", st, ST, "on", this);
            if ((st&ST_ORPHAN) != 0)
                throw new IllegalStateException("releaseResources on orphan");
            st |= ST_RELEASED;
            queryHandlers.remove(this);
            bbView.close();
            bbView = null;
            parser = parser.recycle(this);
            if (queryRope != null)
                queryRope.close();
        }

        protected final void checkReleasedOrOrphan(String action, @Nullable Object actionArg) {
            if ((st&(ST_RELEASED|ST_ORPHAN)) == 0)
                return;
            throw createReleasedOrOrphanEx(action, actionArg);
        }

        protected final IllegalStateException createReleasedOrOrphanEx(String action,
                                                                       @Nullable Object actionArg) {
            String msgNoArg = action+" on released/orphan "+this;
            journal(msgNoArg);
            return new IllegalStateException(msgNoArg + ": " + actionArg);
        }

        @SuppressWarnings("unused") private void doReleaseOrRecycle() {
            try {
                if ((st&ST_ORPHAN) != 0)
                    throw new IllegalStateException("recycling already orphan");
                cleanupAfterEndResponse();
                if ((st & (ST_UNHEALTHY|ST_CLOSING|ST_NOTIFIED_CLOSED|ST_GOT_REQ)) != 0
                        || (st & (ST_RES_STARTED | ST_RES_TERMINATED)) == ST_RES_STARTED) {
                    journal("will not pool, st=", st, ST, "on", this);
                } else {
                    st = ST_ORPHAN;
                    info = null;
                    //noinspection unchecked
                    if (((LIFOPool<QueryHandler<I>>) pool()).offer(this) == null)
                        return;
                    journal("doRecycle", this, ": full pool");
                }
                releaseResources();
            } finally {
                if ((st&(ST_CLOSING|ST_NOTIFIED_CLOSED)) == ST_CLOSING)
                    notifyClosed();
                journal("exit doReleaseOrRecycle", this);
            }
        }

        @Override public void channelRegistered(ChannelHandlerContext ctx) {
            if ((st&ST_ORPHAN) == 0) {
                var ex = new IllegalStateException(journalName() + " registering non-orphan on " + ctx);
                var oldCtx = this.ctx;
                ctx.fireExceptionCaught(ex);
                if (oldCtx != ctx && oldCtx != null)
                    oldCtx.fireExceptionCaught(ex);
            } else {
                st &= ~ST_ORPHAN;
                if (this.st != 0) {
                    onSuspicious("dirty st at channelRegistered");
                    st = 0;
                }
                this.ctx = ctx;
                this.lastCh = ctx.channel();
                this.bbSink.alloc(ctx.alloc());
                this.bsRunnable.executor(ctx.executor());
                ctx.fireChannelRegistered();
            }
        }

        @Override public void channelUnregistered(ChannelHandlerContext ctx) {
            journal("channelUnregistered", this);
            this.bsRunnable.runNow();
            cleanupAfterEndResponse();
            this.bsRunnable.runNow();
            this.ctx = null;
            this.bsRunnable.executor(ForkJoinPool.commonPool());
            if (upstream == null)
                this.bsRunnable.sched(AC_RECYCLE);
            ctx.fireChannelUnregistered();
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            journal("channelInactive", this);
            st |= ST_UNHEALTHY;
            if (!cancel() && (st&ST_CLOSING) != 0)
                notifyClosed();
            ctx.fireChannelInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            st |= ST_UNHEALTHY;
            journal("exception st=", st, ST, cause, "on", this);
            log.error("exceptionCaught({}) for handler={}", cause.getClass().getSimpleName(),
                      this, cause);
            if (errorOrCancelledException == null)
                errorOrCancelledException = cause;
            //if (watchdog != null)
            //    watchdog.stopAndTrigger();
            if (!cancel()) {
                exceptionCaughtWhileUnsubscribed(cause);
            } else if ((st&(ST_GOT_REQ|ST_RES_TERMINATED|ST_RELEASED|ST_ORPHAN)) == ST_GOT_REQ) {
                st |= ST_DIRECT_RES_TERMINATED;
                endResponse(termMessage(cause));
            }
            if (ctx != null)
                ctx.close();
        }

        protected abstract void exceptionCaughtWhileUnsubscribed(Throwable cause);
    }

    private class SparqlHandler extends QueryHandler<FullHttpRequest> {
        private static final byte[] CANCEL_MSG = "query processing was cancelled".getBytes(UTF_8);
        private static final byte[] BAD_UPGRADE_MSG = "Ony possible upgrade is WebSocket".getBytes(UTF_8);
        private static final byte[] BAD_HTTP_VERSION_MSG = "Only HTTP 1.1 is supported".getBytes(UTF_8);
        private static final byte[] BAD_HTTP_ENCODE_MSG = "Could not decode HTTP request".getBytes(UTF_8);
        private static final byte[] NOT_FOUND_MSG = "The requested path was not found, use /sparql for SPARQL protocol and the WebSocket extension".getBytes(UTF_8);
        private static final byte[] NOT_ACCEPTABLE_MSG = ("Cannot satisfy Accept header. Supported types are "
                + Arrays.stream(ResultsSerializer.contentNegotiator().supported())
                .map(MediaType::toString).collect(Collectors.joining(", "))).getBytes(UTF_8);
        private static final byte[] METHOD_NOT_ALLOWED_MSG = "HTTP method not allowed, only HEAD, GET and POST are allowed in SPARQL".getBytes(UTF_8);
        private static final byte[] MISSING_QUERY_GET_MSG = "Missing \"query=\" parameter in GET request".getBytes(UTF_8);
        private static final byte[] MISSING_QUERY_FORM_MSG = "Missing \"query\" parameter in application/x-www-form".getBytes(UTF_8);
        private static final byte[] NOT_FORM_MSG = "Expected Content-Type of request to be application/x-www-form or application/sparql-query".getBytes(UTF_8);

        private @MonotonicNonNull ResultsSerializer<?> serializer;

        @Override protected void serialize(CompressedBatch batch) {
            serializer.serialize(batch.releaseOwnership(this), bbSink.touch(), REC_MAX_FRAME_LEN,
                                 BATCH_RECYCLER, this);
        }

        @Override public void onSerializedChunk(ByteBuf chunk) {
            send(new DefaultHttpContent(chunk));
        }

        @Override protected LIFOPool<SparqlHandler> pool() {return sparqlHandlerPool;}

        @Override protected Object termMessage(@Nullable Throwable cause) {
            HttpContent msg;
            if (cause == null && (st&ST_RES_STARTED) == 0)
                cause = new IllegalStateException("sendTermination() before RES_STARTED");
            if (cause == null) {
                serializer.serializeTrailer(bbSink.touch());
                msg = new DefaultLastHttpContent(bbSink.take());
            } else {
                ByteBuf body;
                if (cause == CancelledException.INSTANCE) {
                    journal("cancel-caused sendTermination on", this);
                    body = wrappedBuffer(CANCEL_MSG);
                } else {
                    journal("sendTermination on", this, "due to", cause.getClass().getSimpleName());
                    try (var tmp = PooledMutableRope.get();
                         var ps = new PrintStream(tmp.asOutputStream())) {
                        cause.printStackTrace(ps);
                        body = NettyRopeUtils.asByteBuf(tmp);
                    }
                }
                if ((st&ST_RES_STARTED) == 0)
                    msg = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, body);
                else  // should be a dead branch
                    msg = new DefaultLastHttpContent(body);
            }
            return msg;
        }

        @Override protected void exceptionCaughtWhileUnsubscribed(Throwable cause) {
            sendRequestError(INTERNAL_SERVER_ERROR, cause,
                    "Unexpected error processing request\n", null, null, null);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            if (handleUpgrade(req))
                return;
            onNewRequest();
            if (req.protocolVersion() != HTTP_1_1) {
                sendRequestError(BAD_REQUEST, BAD_HTTP_VERSION_MSG).close();
            } else if (!req.decoderResult().isSuccess()) {
                sendRequestError(BAD_REQUEST, BAD_HTTP_ENCODE_MSG).close();
            } else if (!req.uri().startsWith(SP_PATH)) {
                sendRequestError(NOT_FOUND, NOT_FOUND_MSG);
            } else if (chooseSerializer(req)) {
                if (req.method().equals(HttpMethod.HEAD))
                    handleHead();
                else if (req.method().equals(HttpMethod. GET))
                    handleGet(req);
                else if (req.method().equals(HttpMethod.POST))
                    handlePost(req);
                else
                    sendRequestError(METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED_MSG);
            }
        }

        private boolean handleUpgrade(HttpRequest request) {
            String upgrade = request.headers().get(UPGRADE);
            if (upgrade == null)
                return false;
            if (upgrade.equalsIgnoreCase("websocket")) {
                if (request instanceof ReferenceCounted r)
                    r.retain(); // both fireChannelRead() and our caller will release
                ctx.fireChannelRead(request);
            } else {
                sendRequestError(BAD_REQUEST, BAD_UPGRADE_MSG);
            }
            return true; // "handled" the request
        }

        private boolean chooseSerializer(FullHttpRequest req) {
            try {
                MediaType mt = negotiator.select(req.headers().valueStringIterator(ACCEPT));
                if (mt != null) {
                    if (serializer != null && !mt.accepts(serializer.contentType()))
                        serializer = serializer.recycle(this);
                    if (serializer == null)
                        serializer = ResultsSerializer.create(mt).takeOwnership(this);
                    return true;
                }
            } catch (Throwable t) {
                log.info("Failed to select and create serializer for accept string {}",
                        req.headers().getAllAsString(ACCEPT), t);
            }
            sendRequestError(NOT_ACCEPTABLE, NOT_ACCEPTABLE_MSG);
            return false;
        }

        private void sendRequestError(HttpResponseStatus st, Throwable cause,
                                      String pre0, Object pre1, Object pre2, Object pre3) {
            if ((this.st&ST_RES_STARTED) != 0) {
                log.error("response already started on sendRequestError({}, {}, {}, {}, {}, {}) on {}",
                          st.code(), cause.getClass().getSimpleName(), pre0, pre1, pre2, pre3, this, cause);
                journal("sendRequestError after started, HTTP", st.code(), "st=", this.st, ST, cause, this);
                if ((this.st&ST_RES_TERMINATED) == 0)
                    endResponse(new DefaultLastHttpContent());
            } else {
                try (var ps = new PrintStream(bbSink.asOutputStream(), false, UTF_8)) {
                    if (pre0 != null) ps.append(pre0);
                    if (pre1 != null) ps.append(pre1.toString());
                    if (pre2 != null) ps.append(pre2.toString());
                    if (pre3 != null) ps.append(pre3.toString());
                    cause.printStackTrace(ps);
                }
                var res = new DefaultFullHttpResponse(HTTP_1_1, st, bbSink.take());
                HttpHeaders headers = res.headers();
                headers.set(CONTENT_TYPE, TEXT_PLAIN_U8)
                        .set(CONTENT_LENGTH, res.content().readableBytes());
                if (SEND_INFO)
                    headers.set("x-fastersparql-info", journalName());
                if ((this.st&ST_RES_STARTED) == 0)
                    startResponse();
                endResponse(res);
            }
        }

        private ChannelHandlerContext sendRequestError(HttpResponseStatus st, byte[] msg) {
            if ((this.st&ST_RES_STARTED) != 0) {
                journal("sendRequestError after started, HTTP", st.code(), "st=", this.st, ST, "on", this);
                log.error("response already started: sendRequestError({}, {}) on {}", st.code(), new String(msg, UTF_8), this);
                if ((this.st&ST_RES_TERMINATED) == 0)
                    endResponse(new DefaultLastHttpContent());
            } else {
                var r = new DefaultFullHttpResponse(HTTP_1_1, st, wrappedBuffer(msg));
                var headers = r.headers();
                if (st == METHOD_NOT_ALLOWED)
                    headers.set(ALLOW, "HEAD, GET, POST");
                if (SEND_INFO)
                    headers.set("x-fastersparql-info", journalName());
                if (msg.length > 0) {
                    headers.set(CONTENT_TYPE, TEXT_PLAIN_U8);
                    headers.set(CONTENT_LENGTH, msg.length);
                }
                if ((this.st&ST_RES_STARTED) == 0)
                    startResponse();
                endResponse(r);
            }
            return ctx;
        }

        private void handleHead() {
            var res = new DefaultFullHttpResponse(HTTP_1_1, OK);
            HttpHeaders headers = res.headers();
            headers.set(ALLOW, "HEAD, GET, POST")
                         .set(CONTENT_TYPE, serializer.contentType())
                         .set(CONTENT_LENGTH, 0);
            if (SEND_INFO)
                headers.set("x-fastersparql-info", journalName());
            startResponse(res, true);
        }

        private static final Pattern QUERY_RX = Pattern.compile("(?i)[?&]query=([^&]+)");
        private void handleGet(FullHttpRequest req) {
            String uri = req.uri();
            info = req.headers().get("x-fastersparql-info");
            var m = QUERY_RX.matcher(uri);
            if (m.find()) {
                String escaped = m.group(1);
                MutableRope unescaped = queryRope(escaped.length());
                unescapeToRope(escaped, unescaped);
                handleQuery(unescaped);
            } else
                sendRequestError(BAD_REQUEST, MISSING_QUERY_GET_MSG);

        }

        private static final byte[] QUERY_EQ = "QUERY=".getBytes(UTF_8);
        private void handlePost(FullHttpRequest req) {
            var reqHeaders = req.headers();
            info = reqHeaders.get("x-fastersparql-info");
            var ct = reqHeaders.get(CONTENT_TYPE);
            var body = bbView.wrap(req.content());
            var queryRope = queryRope(body.len);
            if (indexOfIgnoreCaseAscii(ct, APPLICATION_X_WWW_FORM_URLENCODED, 0) == 0) {
                int begin = 0, len = body.len;
                while (begin < len && !body.hasAnyCase(begin, QUERY_EQ))
                    begin = body.skipUntil(begin, len, '&')+1;
                begin += QUERY_EQ.length;
                if (begin >= len) {
                    sendRequestError(BAD_REQUEST, MISSING_QUERY_FORM_MSG);
                } else {
                    unescape(body, begin, body.skipUntil(begin, body.len, '&'), queryRope);
                    handleQuery(queryRope);
                }
            } else if (indexOfIgnoreCaseAscii(ct, APPLICATION_SPARQL_QUERY, 0) == 0) {
                handleQuery(queryRope.append(body));
            } else {
                sendRequestError(UNSUPPORTED_MEDIA_TYPE, NOT_FORM_MSG);
            }
        }

        private void handleQuery(MutableRope rope) {
            Plan plan;
            //makeWatchdogSpec(rope);
            try {
                plan = parser.parse(rope, 0);
            } catch (Throwable t)  {
                sendRequestError(BAD_REQUEST, t, "Bad query: ",
                        t instanceof InvalidSparqlException e1 ? e1.getMessage()
                              : t.getClass().getSimpleName()+": "+t.getMessage(),
                        "\nQuery:\n", rope);
                return;
            }
            Orphan<? extends Emitter<CompressedBatch, ?>> em;
            try {
                if (useBIt)
                    em = BItEmitter.create(sparqlClient.query(COMPRESSED, plan));
                else
                    em = sparqlClient.emit(COMPRESSED, plan, Vars.EMPTY);
            } catch (Throwable t) {
                sendRequestError(INTERNAL_SERVER_ERROR, t,
                        "Failed to dispatch query: ", rope, "\nReason:", null);
                return;
            }
            try {
                Vars emVars = Emitter.peekVars(em);
                serializer.init(emVars, emVars, false);
                serializer.serializeHeader(bbSink.touch());
                var headers = new DefaultHttpContent(bbSink.take());
                var response = new DefaultHttpResponse(HTTP_1_1, OK);
                HttpHeaders responseHeaders = response.headers();
                responseHeaders.set(CONTENT_TYPE, serializer.contentType())
                               .set(TRANSFER_ENCODING, CHUNKED);
                if (SEND_INFO)
                    responseHeaders.set("x-fastersparql-info", journalName());
                startResponse(response, false);
                send(headers);
            } catch (Throwable t) {
                Emitters.discard(em);
                sendRequestError(INTERNAL_SERVER_ERROR, t, "Failed to build HTTP response ",
                        " and results headers. Reason: ", null, null);
                return;
            }
            subscribeTo(em).request(Long.MAX_VALUE);
        }
    }

    private abstract class WsHandler0 extends QueryHandler<TextWebSocketFrame> {
        protected static final byte[] QUERY      = "!query"     .getBytes(UTF_8);
        protected static final byte[] JOIN       = "!join"      .getBytes(UTF_8);
        protected static final byte[] LEFT_JOIN  = "!left-join" .getBytes(UTF_8);
        protected static final byte[] EXISTS     = "!exists"    .getBytes(UTF_8);
        protected static final byte[] NOT_EXISTS = "!not-exists".getBytes(UTF_8);
        protected static final byte[] MINUS      = "!minus"     .getBytes(UTF_8);
        protected static final byte[] BIND_EMPTY_STREAK = "!bind-empty-streak ".getBytes(UTF_8);
        protected static final byte[] BIND_REQUEST      = "!bind-request ".getBytes(UTF_8);
        protected static final byte[] ERROR             = "!error "  .getBytes(UTF_8);
        protected static final byte[] CANCELLED         = "!cancelled\n"  .getBytes(UTF_8);
        protected static final byte[] END               = "!end\n"        .getBytes(UTF_8);

        private static final int LONG_MAX_VALUE_LEN = String.valueOf(Long.MAX_VALUE).length();

        protected final WsSerializer serializer = WsSerializer.create(ByteBufSink.NORMAL_HINT)
                                                              .takeOwnership(this);
        protected boolean earlyCancel, waitingVars, isBindQuery;
        protected TextWebSocketFrame endFrame = new TextWebSocketFrame(wrappedBuffer(END));
        protected final MutableRope bindReqRope;
        protected final TextWebSocketFrame bindReqFrame;
        protected @Nullable WsServerParser<CompressedBatch> bindingsParser;
        protected final SegmentRopeView tmpView = new SegmentRopeView();
        protected long lastSeqSent;
        protected @MonotonicNonNull TextWebSocketFrame cancelFrame;
        protected long earlyRequest;
        protected BindType bType;
        protected @Nullable Plan query;

        protected WsHandler0() {
            bindReqRope = new MutableRope(
                    BIND_REQUEST.length+LONG_MAX_VALUE_LEN+1+      // !bind-request N\n
                            BIND_EMPTY_STREAK.length+LONG_MAX_VALUE_LEN+1  // !bind-empty-streak S\n
            ).append(BIND_REQUEST);
            bindReqFrame = new TextWebSocketFrame(wrappedBuffer(bindReqRope.u8()));
        }

    }

    private final class WsHandler extends WsHandler0 {
        private static final VarHandle BIND_REQUEST_N;
        static {
            try {
                BIND_REQUEST_N = MethodHandles.lookup().findVarHandle(WsHandler.class, "plainBindRequestN", long.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        private volatile long emptySeq, nonEmptySeq;
        @SuppressWarnings("FieldCanBeLocal") private long plainBindRequestN;

        private final class BindingsCallback
                extends CallbackEmitter<CompressedBatch, BindingsCallback>
                implements Orphan<BindingsCallback> {
            public BindingsCallback(Vars vars) {
                super(COMPRESSED, vars, EMITTER_SVC, RR_WORKER, CREATED, CB_FLAGS);
                if (ResultJournal.ENABLED)
                    ResultJournal.initEmitter(this, vars);
            }

            @Override public BindingsCallback takeOwnership(Object o) {return takeOwnership0(o);}

            @Override protected StringBuilder minimalLabel() {
                return new StringBuilder().append("S.W.BE").append(idTag());
            }

            @Override public void request(long rows) throws NoReceiverException {
                super.request(rows);
                Async.maxRelease(BIND_REQUEST_N, WsHandler.this, requested());
                bsRunnable.sched(AC_BIND_REQ);
            }

            @Override public void rebind(BatchBinding binding) throws RebindException {
                throw new UnsupportedOperationException();
            }

            @Override public    String            toString() { return minimalLabel().toString(); }
            @Override public    Vars          bindableVars() { return Vars.EMPTY; }
            @Override protected void         startProducer() {}
            @Override protected void        cancelProducer() { cancel(true); }
            @Override protected void   earlyCancelProducer() {}
            @Override protected void       releaseProducer() {}
            @Override protected void         pauseProducer() {}
            @Override protected void  resumeProducer(long n) {}
        }

        private final class BindingsBIt extends SPSCBIt<CompressedBatch> {
            private int pending;

            public BindingsBIt(Vars vars) {
                super(COMPRESSED, vars, Math.max(8, COMPRESSED.preferredRowsPerBatch(vars)));
                plainBindRequestN = pending = maxItems;
            }

            @Override protected StringBuilder minimalLabel() {
                return new StringBuilder().append("WS.W.BB:").append(idTag());
            }

            @Override protected boolean mustPark(int offerRows, long queuedRows) {return false;}

            @Override public @Nullable Orphan<CompressedBatch>
            nextBatch(@Nullable Orphan<CompressedBatch> offer) {
                var orphan = super.nextBatch(offer);
                if (orphan != null) {
                    if ((pending -= Batch.peekTotalRows(orphan)) <= maxItems>>1) {
                        Async.maxRelease(BIND_REQUEST_N, WsHandler.this, (long)maxItems);
                        pending = maxItems;
                        bsRunnable.sched(AC_BIND_REQ);
                    }
                }
                return orphan;
            }
        }

        private static final class BindingsParser extends WsServerParser<CompressedBatch> {
            public BindingsParser(CompletableBatchQueue<CompressedBatch> dest, Requestable requestable) {
                super(dest, requestable);
            }

            @Override protected void onPing() { throw new UnsupportedOperationException(); }
        }

        private final class VolatileItBindQuery extends ItBindQuery<CompressedBatch> {
            public VolatileItBindQuery(SparqlQuery query, BindingsBIt bindings, BindType type) {
                super(query, bindings, type);
            }
            @Override public void    emptyBinding(long sequence) {emptySeq    = sequence;}
            @Override public void nonEmptyBinding(long sequence) {nonEmptySeq = sequence;}
        }

        private final class VolatileEmitBindQuery extends EmitBindQuery<CompressedBatch> {
            public VolatileEmitBindQuery(SparqlQuery query, BindingsCallback bindings,
                                         BindType type) {
                super(query, bindings, type);
            }
            @Override public void    emptyBinding(long sequence) {emptySeq    = sequence;}
            @Override public void nonEmptyBinding(long sequence) {nonEmptySeq = sequence;}
        }

        @Override protected void cleanupAfterEndResponse() {
            super.cleanupAfterEndResponse();
            bindingsParser   = null;
            isBindQuery      = false;
            earlyCancel      = false;
            earlyRequest     = -1;
            lastSeqSent      =  0;
            emptySeq         = -1;
            nonEmptySeq      = -1;
        }

        @Override protected void releaseResources() {
            super.releaseResources();
            serializer.recycle(this);
            if (cancelFrame != null && cancelFrame.refCnt() > 0)
                cancelFrame.release();
            if (endFrame != null && endFrame.refCnt() > 0)
                endFrame.release();
            bindReqRope.close();
        }

        @Override protected LIFOPool<WsHandler> pool() {return wsHandlerPool;}

        @Override protected Object termMessage(@Nullable Throwable cause) {
            TextWebSocketFrame frame;
            if (cause == null) {
                if (endFrame.refCnt() == 1)
                    (frame = endFrame.retain()).content().readerIndex(0);
                else
                    endFrame = frame = new TextWebSocketFrame(wrappedBuffer(END));
            } else if (cause == CancelledException.INSTANCE) {
                if (cancelFrame == null || cancelFrame.refCnt() != 1)
                    cancelFrame = frame = new TextWebSocketFrame(wrappedBuffer(CANCELLED));
                else
                    (frame = cancelFrame.retain()).content().readerIndex(0);
            } else {
                bbSink.touch().append(ERROR).append(cause.getClass().getSimpleName())
                              .append(Objects.toString(cause.getMessage()).replace("\n", "\\n"))
                              .append('\n');
                ByteBuf content = bbSink.take();
                if (ThreadJournal.ENABLED) {
                    int end = Math.min(40, content.readableBytes()-1);
                    journal("!error st=", st, ST, "on", this,
                            content.toString(ERROR.length, end, UTF_8));
                }
                frame = new TextWebSocketFrame(content);
            }
            return frame;
        }

        @Override protected void exceptionCaughtWhileUnsubscribed(Throwable cause) {
            sendRequestError("Unexpected exception", cause);
        }

        private void sendRequestError(Object o0, Object o1) {
            bbSink.touch().append(ERROR);
            if (o0 != null) bbSink.append(o0.toString().replace("\n", "\\n"));
            if (o1 != null) bbSink.append(o1.toString().replace("\n", "\\n"));
            bbSink.append('\n');
            ByteBuf content = bbSink.take();
            if (ThreadJournal.ENABLED) {
                int end = Math.min(40, content.readableBytes()-1);
                journal("!error st=", st, ST, "on", this,
                        content.toString(ERROR.length, end, UTF_8));
            }
            if ((st&ST_RES_STARTED) == 0)
                startResponse();
            endResponse(new TextWebSocketFrame(content));
        }

        @Override protected void serialize(CompressedBatch batch) {
            if (isBindQuery && batch.rows > 0) {
                var tail = batch.tail();
                long seq = tail.localView(tail.rows-1, 0, tmpView)
                         ? WsBindingSeq.parse(tmpView, 0, tmpView.len) : -1L;
                if (seq > lastSeqSent)
                    lastSeqSent = seq;
                else if (seq < lastSeqSent)
                    invalidLastSeqSent(seq);
            }
            if (!batch.validate())
                throw new IllegalStateException(render(batch, "corrupted on", this));
            serializer.serialize(batch.releaseOwnership(this), bbSink.touch(), REC_MAX_FRAME_LEN,
                                 BATCH_RECYCLER, this);
        }

        @Override public void onSerializedChunk(ByteBuf chunk) {
            send(new TextWebSocketFrame(chunk));
        }

        private void invalidLastSeqSent(long seq) {
            if (seq == -1) {
                journal("Missing binding seq, handler=", this);
                throw new IllegalStateException("Missing binding seq");
            } else {
                journal("non-monotonic lastSeqSent from", lastSeqSent, "to", seq, "on", this);
                throw new IllegalStateException("non-monotonic step of lastSeqSent from "+lastSeqSent+" to "+seq+" on "+this);
            }
        }


        @Override
        protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
            checkReleasedOrOrphan("read0", frame);
            SegmentRope msg = bbView.wrapAsSingle(frame.content());
            byte f = msg.len < 2 ? 0 : msg.get(1);
            if (f == 'c' && msg.has(0, AbstractWsParser.CANCEL_LF)) {
                readCancel();
            } else if (f == 'r' && msg.has(0, AbstractWsParser.REQUEST)) {
                readRequest(msg);
            } else if ((f != 'i' && f != 'p') || !readUnusual(msg)) {
                if (waitingVars)
                    readVarsFrame(msg);
                else if (bindingsParser != null)
                    readBindings(bindingsParser, msg);
                else
                    handleQueryCommand(msg, f);
            }
        }

        private boolean readUnusual(SegmentRope msg) {
            if (msg.has(0, AbstractWsParser.INFO)) {
                int eol = msg.skipUntil(0, msg.len, '\n');
                info = msg.toString(AbstractWsParser.INFO.length, eol);
                journal("!info ", info, "on", this);
            } else if (msg.has(0, AbstractWsParser.PING)) {
                send(bbSink.touch().append(AbstractWsParser.PING_ACK).take());
            } else if (msg.has(0, AbstractWsParser.PING_ACK)) {
                journal("got !ping-ack on", this);
            } else {
                return false; // not an unusual command
            }
            return true; // handled
        }

        private void readCancel() {
            if (cancel())
                return;
            if (waitingVars)
                earlyCancel = true;
            else
                journal("ignoring early/late !cancel on", this);
        }

        private void readRequest(SegmentRope msg) {
            int start = AbstractWsParser.REQUEST.length;
            long n;
            try {
                start = msg.skipWS(start, msg.len);
                n = msg.hasAnyCase(start, AbstractWsParser.MAX)
                        ? Long.MAX_VALUE : msg.parseLong(start);
            } catch (NumberFormatException t) {
                int eol = msg.skip(start, msg.len, Rope.UNTIL_WS);
                sendRequestError("Invalid control message", msg.toString(0, eol));
                return;
            }
            if (waitingVars)
                earlyRequest = n;
            else if ((st&ST_GOT_REQ) != 0)
                request(n);
            else
                journal("ignoring late/early !request on", this);
        }

        private void abortReadVarsFrame() {
            if (earlyCancel)  {
                if ((st&ST_RES_STARTED) == 0)
                    startResponse();
                endResponse(termMessage(CancelledException.INSTANCE));
            } else {
                Object m;
                if      (query == null)         m = "No query set at readVarsFrame()";
                else if (serverClosing != null) m = serverClosing;
                else if ((st&ST_CLOSING) != 0)  m = "Server close()d server-side handler";
                else                            m = "bindings vars frame missing LF (\\n)";
                sendRequestError(m, null);
            }
        }
        private void readVarsFrame(SegmentRope msg) {
            int len = msg.len, eol = msg.skipUntil(0, len, '\n');
            if (query == null || earlyCancel || (st&ST_CLOSING) != 0 || eol == len) {
                abortReadVarsFrame();
                return;
            }

            Orphan<? extends Emitter<CompressedBatch, ?>> em = null;
            try {
                // artificially insert BINDING_SEQ_VARNAME as 0-th binding var. WsServerParser
                // will transparently assign values during parsing.
                var bindingsVars = new Vars.Mutable(10);
                bindingsVars.add(WsBindingSeq.VAR);
                for (int i = 0, j; i < eol; i = j+1) {
                    byte c = msg.get(i);
                    if (c != '?' && c != '$') {
                        sendRequestError("No var marker for var", null);
                        return;
                    }
                    j = msg.skipUntil(i, len, '\t',  '\n');
                    bindingsVars.add(asFinal(msg, i+1, j));
                }
                waitingVars = false;

                if (useBIt) {
                    var bindings = new BindingsBIt(bindingsVars);
                    var bq = new VolatileItBindQuery(query, bindings, bType);
                    bindingsParser = new BindingsParser(bindings, this);
                    em = BItEmitter.create(sparqlClient.query(bq));
                } else {
                    var bindings = new BindingsCallback(bindingsVars);
                    var bq = new VolatileEmitBindQuery(query, bindings, bType);
                    bindingsParser = new BindingsParser(bindings, this);
                    em = sparqlClient.emit(bq, Vars.EMPTY);
                }
                bindingsParser.namer(BIND_PARSER_NAMER, this);

                // only send binding seq number and right unbound vars
                Vars all = Emitter.peekVars(em);
                var serializeVars = new Vars.Mutable(all.size()+1);
                serializeVars.add(WsBindingSeq.VAR);
                for (var v : all)
                    if (!bindingsVars.contains(v)) serializeVars.add(v);
                startResponse(headersFrame(all, serializeVars), earlyRequest <= 0);
                subscribeTo(em);
            } catch (Throwable t) {
                Emitters.discard(em);
                sendRequestError("Could not dispatch query: ", t.toString());
                log.debug("Could not dispatch bind query on {}", this, t);
                return;
            }
            if (useBIt)
                doBindReq();
            if (earlyRequest > 0) {
                request(earlyRequest);
                earlyRequest = -1;
            }
            readBindings(bindingsParser, msg);
        }
        private static final ResultsParser.Namer<WsHandler> BIND_PARSER_NAMER = (ignored, h) -> {
            Channel ch = h.channelOrLast();
            var sb = new StringBuilder().append("ws-bind:");
            sb.append(ch == null ? "null" : ch.id().asShortText());
            if (h.info != null)
                sb.append("<-").append(h.info);
            return sb.toString();
        };

        private TextWebSocketFrame headersFrame(Vars all, Vars serialize) {
            serializer.init(all, serialize, false);
            serializer.serializeHeader(bbSink.touch());
            return new TextWebSocketFrame(bbSink.take());
        }

        private void readBindings(WsServerParser<CompressedBatch> bindingsParser,
                                  SegmentRope msg) {
            try {
                bindingsParser.feedShared(msg);
            } catch (BatchQueue.TerminatedException|CancelledException ignored) {}
        }

        @Override protected void doBindReq() {
            long n;
            if (!isBindQuery || (st&(ST_RES_STARTED|ST_RES_TERMINATED)) != ST_RES_STARTED) {
                journal("stale/early doBindReq", this);
            } else if (bindReqFrame.refCnt() > 1) {
                retryBindReq();
            } else if ((n=(long)BIND_REQUEST_N.getAndSetAcquire(this, 0)) > 0) {
                bindReqRope.len = BIND_REQUEST.length;
                if (n == Long.MAX_VALUE) bindReqRope.append(AbstractWsParser.MAX);
                else                     bindReqRope.append(n);
                bindReqRope.append((byte)'\n');
                long emptySeq = this.emptySeq;
                if (lastSeqSent == nonEmptySeq && emptySeq > lastSeqSent && lastSeqSentDone())
                    bindReqRope.append(BIND_EMPTY_STREAK).append(emptySeq).append((byte)'\n');
                assert bindReqFrame.content().array() == bindReqRope.utf8 : "rope grown";
                bindReqFrame.content().readerIndex(0).writerIndex(bindReqRope.len);
                send(bindReqFrame.retain());
            }
        }
        private void retryBindReq() {
            journal("frame in-use, retrying doBindReq on", this);
            bsRunnable.sched(AC_BIND_REQ);
        }
        private boolean lastSeqSentDone() {
            while ((int)Q.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
            CompressedBatch q = sendQueue;
            boolean done = q != null && q.rows > 0 && q.localView(0, 0, tmpView)
                    && WsBindingSeq.parse(tmpView, 0, tmpView.len) > lastSeqSent;
            Q.setRelease(this, 0);
            return done;
        }

        private void handleQueryCommand(SegmentRope msg, byte f) {
            byte[] ex = null;
            boolean isBindQuery = true;
            switch (f) {
                case 'q' -> { bType = BindType.JOIN;       ex = QUERY; isBindQuery = false; }
                case 'j' -> { bType = BindType.JOIN;       ex = JOIN; }
                case 'l' -> { bType = BindType.LEFT_JOIN;  ex = LEFT_JOIN; }
                case 'e' -> { bType = BindType.EXISTS;     ex = EXISTS; }
                case 'n' -> { bType = BindType.NOT_EXISTS; ex = NOT_EXISTS; }
                case 'm' -> { bType = BindType.MINUS;      ex = MINUS; }
            }
            onNewRequest();
            if (ex == null || !msg.has(0, ex)) {
                var cmd = msg.sub(0, msg.skipUntil(0, msg.len, '\n', ' '));
                sendRequestError("Unexpected command: ", cmd);
                return;
            }
            this.waitingVars    = isBindQuery;
            this.isBindQuery    = isBindQuery;
            this.bindingsParser = null;
            var sparql = queryRope(msg.len).append(msg, ex.length, msg.len);
            //makeWatchdogSpec(sparql);
            try {
                query = parser.parse(sparql, 0);
            } catch (Throwable t) {
                query = null;
                var reason = (t instanceof InvalidSparqlException e ? e.getMessage():t.toString());
                sendRequestError("Could not parse query: ", reason);
                return;
            }
            if (SEND_INFO) {
                bbSink.touch().append("!info ").append(journalName()).append('\n');
                ctx.writeAndFlush(new TextWebSocketFrame(bbSink.take()));
            }
            if (isBindQuery)
                return;

            Orphan<? extends Emitter<CompressedBatch, ?>> em = null;
            try {
                if (useBIt) {
                    em = BItEmitter.create(sparqlClient.query(COMPRESSED, query));
                } else {
                    em = sparqlClient.emit(COMPRESSED, query, Vars.EMPTY);
                }
                Vars vars = Emitter.peekVars(em);
                startResponse(headersFrame(vars, vars), true);
                subscribeTo(em);
            } catch (Throwable t) {
                log.debug("Could not dispatch query on {}", this, t);
                Emitters.discard(em);
                sendRequestError("Could not dispatch query", t.toString());
            }
        }
    }
}
