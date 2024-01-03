package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.netty.util.*;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.async.CallbackEmitter;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.ContentNegotiator;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.PlainRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.WsBindingSeq;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.WsServerParser;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.ReferenceCounted;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.CompressedBatchType.COMPRESSED;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.util.UriUtils.unescape;
import static com.github.alexishuf.fastersparql.util.UriUtils.unescapeToRope;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED;
import static io.netty.handler.codec.http.HttpHeaderValues.CHUNKED;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.util.AsciiString.indexOfIgnoreCaseAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Expose a {@link SparqlClient} (which includes a {@link Federation}) through the SPARQL
 * protocol (HTTP) and the custom WebSocket protocol.
 */
public class NettyEmitSparqlServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NettyEmitSparqlServer.class);
    private static final String SP_PATH = "/sparql";
    private static final String APPLICATION_SPARQL_QUERY = "application/sparql-query";
    private static final String TEXT_PLAIN_U8 = "text/plain; charset=utf-8";

    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup workerGroup;
    private final Channel server;
    private final SparqlClient sparqlClient;
    private final ContentNegotiator negotiator = ResultsSerializer.contentNegotiator();
    private int serverWsSizeHint = WsSerializer.DEF_BUFFER_HINT;
    private final SparqlClient.@Nullable Guard sparqlClientGuard;

    public NettyEmitSparqlServer(SparqlClient sparqlClient, boolean sharedSparqlClient,
                                 String host, int port) {
        this.sparqlClient = sparqlClient;
        this.sparqlClientGuard = sharedSparqlClient ? sparqlClient.retain() : null;
        acceptGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
//        String debuggerName = sparqlClient.endpoint().toString();
        server = new ServerBootstrap().group(acceptGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("http", new HttpServerCodec())
//                                .addLast("debug", new NettyChannelDebugger(debuggerName))
//                                .addLast("log", new LoggingHandler(NettySparqlServer.class, LogLevel.INFO, ByteBufFormat.HEX_DUMP))
                                .addLast("req-aggregator", new HttpObjectAggregator(1<<15, true))
                                .addLast("keepalive", new HttpServerKeepAliveHandler())
//                              .addLast("ws-compression", new WebSocketServerCompressionHandler())
                                .addLast("sparql", new SparqlHandler())
                                .addLast("ws", new WebSocketServerProtocolHandler(SP_PATH, null, true))
                                .addLast("ws-sparql", new WsHandler());
                    }
                }).bind(host, port).syncUninterruptibly().channel();
    }

    /** Get the TCP port where the server is listening to connections. */
    public int port() {
        return ((InetSocketAddress)server.localAddress()).getPort();
    }

    /** Get the {@link InetSocketAddress} where the server is listening for connections. */
    @SuppressWarnings("unused") public InetSocketAddress listenAddress() {
        return (InetSocketAddress)server.localAddress();
    }

    @Override public String toString() {
        return "NettyEmitSparqlServer"+server+"("+sparqlClient.endpoint()+")";
    }

    @SuppressWarnings("unused") @Override public void close()  {
        CountDownLatch latch = new CountDownLatch(3);
        server.close().addListener(f -> {
            latch.countDown();
            acceptGroup.shutdownGracefully(10, 50, MILLISECONDS)
                       .addListener(f2 -> {
                           latch.countDown();
                           workerGroup.shutdownGracefully(10, 50, MILLISECONDS)
                                      .addListener(f3 -> latch.countDown());
                       });
        });
        try {
            if (!latch.await(10, TimeUnit.SECONDS))
                log.warn("{} is taking too long to shutdown, leaking.", this);
        } catch (InterruptedException e) {
            log.warn("Interrupted while closing {}, leaking.", this);
            Thread.currentThread().interrupt();
        } finally {
            if (sparqlClientGuard == null) sparqlClient.close();
            else                           sparqlClientGuard.close();
        }
    }

    /* --- --- --- handlers --- --- --- */

    private abstract static class Sender<M> extends NettyResultsSender<M>
            implements Receiver<CompressedBatch> {
        private @Nullable Throwable abortCause;
        protected final Emitter<CompressedBatch> upstream;
        private final QueryHandler<?, M> handler;

        private Sender(ResultsSerializer serializer, QueryHandler<?, M> handler,
                      Emitter<CompressedBatch> upstream) {
            super(serializer, handler.ctx);
            this.handler = handler;
            this.upstream = upstream;
            upstream.subscribe(this);
        }

        private static final CompleteAction<?> COMPLETE_ACTION = new CompleteAction<>();
        private static final class CompleteAction<M> extends Action {
            public CompleteAction() {super("COMPLETE");}
            @Override public void run(NettyResultsSender<?> sender) {
                @SuppressWarnings("unchecked") var mSender = (Sender<M>) sender;
                mSender.handler.endQuery(mSender, OK, false, null);
            }
        }
        private static final CancelAction<?> CANCEL_ACTION = new CancelAction<>();
        private static final class CancelAction<M> extends Action {
            public CancelAction() {super("CANCEL");}
            @Override public void run(NettyResultsSender<?> sender) {
                @SuppressWarnings("unchecked") var mSender = (Sender<M>)sender;
                mSender.handler.endQuery(mSender, PARTIAL_CONTENT, true, null);
            }
        }
        private static final class ErrorAction<M> extends Action {
            private final Throwable error;
            private ErrorAction(Throwable error) {
                super("ERROR");
                this.error = error;
            }
            @Override public void run(NettyResultsSender<?> sender) {
                @SuppressWarnings("unchecked") var mSender = (Sender<M>)sender;
                mSender.handler.endQuery(mSender, INTERNAL_SERVER_ERROR, false, error);
            }
        }

        /* --- --- ---  methods --- --- --- */

        public void start() {
            Vars vars = upstream.vars();
            sendInit(vars, vars, false);
            upstream.request(Long.MAX_VALUE);
        }

        public final void abort(Throwable cause) {
            this.abortCause = cause;
            this.upstream.cancel();
        }

        /* --- --- --- NettyResultsSender methods --- --- --- */

        @Override public final void sendCancel() { throw new UnsupportedOperationException(); }

        /* --- --- --- Receiver methods --- --- --- */

        @Override public Stream<? extends StreamNode> upstreamNodes() {return Stream.of(upstream);}
        @Override public final @Nullable CompressedBatch onBatch(CompressedBatch batch) {
            sendSerializedAll(batch);
            return batch;
        }
        @Override public void onComplete() {
            journal("onComplete, sender=", this);
            sendTrailer();
            execute(COMPLETE_ACTION);
        }
        @Override public final void onCancelled()        {
            journal("onCancelled, abortCause=", abortCause, "sender=", this);
            execute(abortCause==null ? CANCEL_ACTION : new ErrorAction<>(abortCause));
        }
        @Override public final void onError(Throwable e) {
            journal("onError", e, "sender=", this);
            execute(new ErrorAction<>(e));
        }
    }

    private static final class HttpSender extends Sender<HttpContent> {
        public HttpSender(ResultsSerializer serializer, QueryHandler<?, HttpContent> handler,
                          Emitter<CompressedBatch> upstream) {
            super(serializer, handler, upstream);
        }
        @Override protected HttpContent wrap    (ByteBuf b) {return new DefaultHttpContent    (b);}
        @Override protected HttpContent wrapLast(ByteBuf b) {return new DefaultLastHttpContent(b);}
    }

    private static final class WsSender extends Sender<TextWebSocketFrame> {
        private final WsHandler handler;
        private Vars serializeVars;
        private volatile long lastSentSeq = -1;

        public WsSender(ResultsSerializer serializer,
                        WsHandler handler,
                        Emitter<CompressedBatch> upstream) {
            super(serializer, handler, upstream);
            this.handler = handler;
            serializeVars = upstream.vars();
        }
        @Override protected TextWebSocketFrame wrap(ByteBuf b) {return new TextWebSocketFrame(b);}

        @Override public void start() {
            sendInit(upstream.vars(), serializeVars, false);
            upstream.request(Long.MAX_VALUE);
        }

        boolean canSendEmptyStreak() { // runs on event loop
            lock(); // required due to race on this.active
            try {
                VolatileBindQuery bindQuery = handler.bindQuery;
                if (bindQuery == null || active) return false;
                long lastSent = lastSentSeq;
                return lastSent == bindQuery.nonEmptySeq && bindQuery.emptySeq > lastSent;
            } finally { unlock(); }
        }

        @Override public void sendSerializedAll(Batch<?> batch) {
            super.sendSerializedAll(batch);
            if (handler.bindQuery == null || batch.rows == 0)
                return;
            var tmpView = handler.tmpView;
            Batch<?> tail = batch.tail();
            if (!tail.localView(tail.rows-1, 0, tmpView))
                throw new IllegalStateException("Missing binding sequence");
            lastSentSeq = WsBindingSeq.parse(tmpView, 0, tmpView.len);
        }

        @Override public void sendTrailer() {
            handler.adjustSizeHint(sink.sizeHint());
            super.sendTrailer();
        }
    }

    private static class VolatileBindQuery extends EmitBindQuery<CompressedBatch> {
        volatile long nonEmptySeq = -1, emptySeq = -1;
        public VolatileBindQuery(SparqlQuery query, Emitter<CompressedBatch> bindings, BindType type) {
            super(query, bindings, type);
        }
        @Override public void    emptyBinding(long sequence) {    emptySeq = sequence; }
        @Override public void nonEmptyBinding(long sequence) { nonEmptySeq = sequence; }
    }

    private abstract class QueryHandler<I, O> extends SimpleChannelInboundHandler<I>
                                              implements ChannelBound {
        private final SparqlParser sparqlParser = new SparqlParser();
        protected @MonotonicNonNull ChannelHandlerContext ctx;
        protected final ByteBufRopeView bbRopeView = ByteBufRopeView.create();
        protected @Nullable Plan query;
        protected @Nullable Sender<O> sender;

        @Override public String toString() {
            var parent = NettyEmitSparqlServer.this;
            if (ctx == null) {
                return parent +"."+getClass().getSimpleName()+'@'
                        +Integer.toHexString(System.identityHashCode(this))+"[UNREGISTERED]";
            }
            return parent+ctx.channel().toString();
        }

        @Override public @Nullable Channel channel() {
            return ctx == null ? null : ctx.channel();
        }

        /* --- --- --- channel events that do not depend on SPARQL protocol variant--- --- --- */

        @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;
            super.channelRegistered(ctx);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            journal("channelInactive, handler=", this);
            var sender = this.sender;
            if (sender != null)
                sender.abort(new FSException("channel closed"));
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("Exception for handler={}", this, cause);
            journal("exceptionCaught, handler=", this, "cause=", cause);
            var sender = this.sender;
            if (sender != null)
                sender.abort(cause);
            super.exceptionCaught(ctx, cause);
        }

        /* --- --- --- helpers --- --- --- */

        protected Plan parseQuery(SegmentRope sparql) {
            try {
                return query = sparqlParser.parse(sparql, 0);
            } catch (Throwable e) {
                var ex = new NoTraceException("Invalid query syntax: "+e.getMessage()
                        +"\nQuery:\n"+sparql.toString().replace("\n", "\n  "));
                endQuery(sender, BAD_REQUEST, false, ex);
            }
            return null;
        }

        /* --- --- --- abstract methods for subclasses --- --- --- */

        /**
         * Called only once per query. Arguments indicate three possible scenarios:
         *
         * <ol>
         *     <li>All results have been already serialized to the client and there was
         *         no error during query processing.</li>
         *     <li>There was no error during query processing, but not all results were serialized
         *     since query processing stopped early due to a {@link Emitter#cancel()}</li>
         *     <li>There was an error during query processing</li>
         * </ol>
         *
         * <p><strong>This method must be called from the event loop.</strong></p>
         *
         * @param sender the {@link Sender} calling this method. If null,
         *                   {@code error} must be non-null, and it will indicate an issue with
         *                   the query or with its dispatching.
         * @param status The {@link HttpResponseStatus} to use if building an HTTP response
         * @param cancelled whether {@link Emitter#cancel()} was called
         * @param error the error that caused query processing to fail or {@code null} if
         *              {@code cancelled} or the query completed without errors.
         */
        protected abstract void endQuery(@Nullable Sender<O> sender,
                                         HttpResponseStatus status,
                                         boolean cancelled,
                                         @Nullable Throwable error);
    }

    private static final class NoTraceException extends Exception {
        public NoTraceException(String message) {super(message);}
    }


    private final class SparqlHandler extends QueryHandler<FullHttpRequest, HttpContent> {
        private static final NoTraceException BAD_HTTP_VERSION_EX = new NoTraceException("Bad HTTP version, only HTTP/1.1 is supported");
        private static final NoTraceException BAD_HTTP_ENCODE_EX = new NoTraceException("Could not decode HTTP request");
        private static final NoTraceException NOT_FOUND_EX = new NoTraceException("Not found, HTTP sparql endpoint is at "+SP_PATH);
        private static final NoTraceException BAD_METHOD_EX = new NoTraceException("Only GET and POST are allowed");
        private static final NoTraceException NO_CONTENT_TYPE = new NoTraceException("Cannot satisfy Accept header. Supported types are "
                + Arrays.stream(ResultsSerializer.contentNegotiator().supported())
                .map(MediaType::toString).collect(Collectors.joining(", ")));
        private static final NoTraceException BAD_UPGRADE_EX = new NoTraceException("Ony possible upgrade is WebSocket");
        private static final NoTraceException MISSING_QUERY_GET_EX = new NoTraceException("Missing \"query\" parameter in GET request");
        private static final NoTraceException MISSING_QUERY_FORM_EX = new NoTraceException("Missing \"query\" parameter in form request");
        private static final NoTraceException NOT_FORM_EX = new NoTraceException("Expected Content-Type to be application/x-www-form-urlencoded or application/sparql-query");

        private boolean responseStarted;
        private ResultsSerializer resultsSerializer;

        /* --- --- --- implement QueryHandler --- --- --- */

        @Override public String journalName() {
            return "S.SH:"+(ctx == null ? "null" : ctx.channel().id().asShortText());
        }

        @Override protected void endQuery(@Nullable Sender<HttpContent> sender,
                                          HttpResponseStatus status,
                                          boolean cancelled, @Nullable Throwable error) {
            if (ThreadJournal.ENABLED) {
                journal("endQuery handler=", this, "error=", error);
                journal("endQuery cancelled=", cancelled ? 1 : 0);
            }
            if (this.sender != sender) {
                log.warn("Received endQuery() from {}, current sender is {}", sender, this.sender);
                return;
            }
            try {
                if (cancelled && error == null)
                    error = new FSCancelledException();
                if (error != null) {
                    var msg = new ByteRope(512);
                    msg.append("Could not process query due to ")
                            .append(error.getClass().getSimpleName());
                    if (query != null) {
                        msg.append("\nQuery:\n").append(query.toString().replace("\n", "\n  "));
                        if (msg.charAt(msg.length() - 3) != '\n') msg.append('\n');
                    }
                    try (var ps = new PrintStream(msg.asOutputStream())) {
                        error.printStackTrace(ps);
                    }
                    var msgBB = Unpooled.wrappedBuffer(msg.u8(), 0, msg.len);
                    Object hc;
                    if (responseStarted) {
                        hc = new DefaultLastHttpContent(msgBB);
                    } else {
                        var r = new DefaultFullHttpResponse(HTTP_1_1, status, msgBB);
                        r.headers().set(CONTENT_TYPE, TEXT_PLAIN_U8)
                                   .set(CONTENT_LENGTH, msgBB.readableBytes());
                        hc = r;
                    }
                    ctx.writeAndFlush(hc);
                }
            } finally {
                this.query = null;
                this.sender = null;
            }
        }

        /* --- --- --- handle SPARQL HTTP protocol --- --- --- */

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            if (handleUpgrade(req))
                return;
            journal("server channelRead0, handler=", this, "prev sender=", sender);
            responseStarted = false;
            sender = null;
            HttpMethod method = req.method();
            if (req.protocolVersion() != HTTP_1_1) {
                endQuery(null, BAD_REQUEST, false, BAD_HTTP_VERSION_EX);
            } else if (!req.decoderResult().isSuccess()) {
                endQuery(null, BAD_REQUEST, false, BAD_HTTP_ENCODE_EX);
            } else if (!req.uri().startsWith(SP_PATH)) {
                endQuery(null, NOT_FOUND, false, NOT_FOUND_EX);
            } else if (badMethod(method)) {
                endQuery(null, METHOD_NOT_ALLOWED, false, BAD_METHOD_EX);
            } else if (chooseSerializer(req)) {
                if      (method.equals(HttpMethod.HEAD)) handleHead(req);
                else if (method.equals(HttpMethod.GET )) handleGet(req);
                else if (method.equals(HttpMethod.POST)) handlePost(req);
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
                endQuery(sender, BAD_REQUEST, false, BAD_UPGRADE_EX);
            }
            return true; // "handled" the request
        }

        private void handleHead(FullHttpRequest req) {
            var resp = new DefaultFullHttpResponse(req.protocolVersion(), OK);
            resp.headers().set(CONTENT_TYPE, resultsSerializer.contentType())
                    .set(CONTENT_LENGTH, 0)
                    .set(ALLOW, "GET, POST, HEAD");
            ctx.writeAndFlush(resp);
            responseStarted = false;
        }

        private static final Pattern QUERY_RX = Pattern.compile("(?i)[?&]query=([^&]+)");
        private void handleGet(HttpRequest req) {
            String uri = req.uri();
            var m = QUERY_RX.matcher(uri);
            if (m.find())
                handleQuery(unescapeToRope(m.group(1)));
            else
                endQuery(null, BAD_REQUEST, false, MISSING_QUERY_GET_EX);
        }

        private static final byte[] QUERY_EQ = "QUERY=".getBytes(UTF_8);
        private void handlePost(FullHttpRequest req) {
            String ct = req.headers().get(CONTENT_TYPE);
            PlainRope body = bbRopeView.wrap(req.content());
            if (indexOfIgnoreCaseAscii(ct, APPLICATION_X_WWW_FORM_URLENCODED, 0) == 0) {
                int begin = 0, len = body.len;
                while (begin < len && !body.hasAnyCase(begin, QUERY_EQ))
                    begin = body.skipUntil(begin, len, '&')+1;
                begin += QUERY_EQ.length;
                if (begin >= len)
                    endQuery(null, BAD_REQUEST, false, MISSING_QUERY_FORM_EX);
                else
                    handleQuery(unescape(body, begin, body.skipUntil(begin, body.len, '&')));
            } else if (indexOfIgnoreCaseAscii(ct, APPLICATION_SPARQL_QUERY, 0) == 0) {
                handleQuery(new ByteRope(body));
            } else {
                endQuery(null, UNSUPPORTED_MEDIA_TYPE, false, NOT_FORM_EX);
            }
        }

        private void handleQuery(SegmentRope sparqlRope) {
            Plan query;
            if ((query = parseQuery(sparqlRope)) == null)
                return;
            var results = sparqlClient.emit(COMPRESSED, query, Vars.EMPTY);
            journal("parsed query, handler=", this, "emitter=", results);
            if (ThreadJournal.ENABLED)
                journal("channel=", ctx.channel().toString());
            sender = new HttpSender(resultsSerializer, this, results);
            var res = new DefaultHttpResponse(HTTP_1_1, OK);
            res.headers().set(CONTENT_TYPE, resultsSerializer.contentType())
                         .set(TRANSFER_ENCODING, CHUNKED);
            responseStarted = true;
            ctx.write(res);
            sender.start();
        }

        private static boolean badMethod(HttpMethod method) {
            return !method.equals(HttpMethod.GET) && !method.equals(HttpMethod.HEAD)
                    && !method.equals(HttpMethod.POST);
        }

        private boolean chooseSerializer(HttpRequest req) {
            try {
                MediaType mt = negotiator.select(req.headers().valueStringIterator(ACCEPT));
                if (mt != null) {
                    resultsSerializer = ResultsSerializer.create(mt);
                    return true;
                }
            } catch (Throwable t) {
                log.info("Failed to select and create serializer for accept string {}",
                         req.headers().getAllAsString(ACCEPT), t);
            }
            endQuery(null, NOT_ACCEPTABLE, false, NO_CONTENT_TYPE);
            return false;
        }
    }

    private final class WsHandler extends QueryHandler<WebSocketFrame, TextWebSocketFrame>
            implements WsFrameSender<ByteBufSink, ByteBuf> {
        private static final byte[] CANCEL     = "!cancel"    .getBytes(UTF_8);
        private static final byte[] QUERY      = "!query"     .getBytes(UTF_8);
        private static final byte[] JOIN       = "!join"      .getBytes(UTF_8);
        private static final byte[] LEFT_JOIN  = "!left-join" .getBytes(UTF_8);
        private static final byte[] EXISTS     = "!exists"    .getBytes(UTF_8);
        private static final byte[] NOT_EXISTS = "!not-exists".getBytes(UTF_8);
        private static final byte[] MINUS      = "!minus"     .getBytes(UTF_8);
        private static final byte[] BIND_EMPTY_STREAK = "!bind-empty-streak ".getBytes(UTF_8);

        private static final TextWebSocketFrame CANCELLED = new TextWebSocketFrame("!cancelled\n");

        private static final NoTraceException READ_VARS_NO_QUERY_EX = new NoTraceException("attempt to read bindings vars before query command");
        private static final NoTraceException VARS_NO_LF_EX = new NoTraceException("bindings vars frame does nto end in \\n (LF, 0x0A)");
        private static final NoTraceException NO_VAR_MARKER_EX = new NoTraceException("missing ?/$ marker in var name");

        private final ByteRope tmpSeq = new ByteRope(BIND_EMPTY_STREAK.length+12).append(BIND_EMPTY_STREAK);
        private final SegmentRope tmpView = new SegmentRope();
        private @Nullable WsServerParser<CompressedBatch> bindingsParser;
        private BindType bType = BindType.JOIN;
        private @Nullable VolatileBindQuery bindQuery;
        private byte @MonotonicNonNull [] fullBindReq, halfBindReq;
        private volatile long requestBindingsAt;
        private final int maxBindings = FSProperties.wsServerBindings();
        private int sizeHint;
        private boolean clientCancelled, waitingVars;

        private void adjustSizeHint(int observed) {
            sizeHint = ByteBufSink.adjustSizeHint(sizeHint, observed);
            serverWsSizeHint = ByteBufSink.adjustSizeHint(serverWsSizeHint, sizeHint);
        }

        /* --- --- --- implement QueryHandler --- --- --- */

        @Override public String journalName() {
            return "S.WH:"+(ctx == null ? "null" : ctx.channel().id().asShortText());
        }

        @Override protected void endQuery(@Nullable Sender<TextWebSocketFrame> sender,
                                          HttpResponseStatus status, boolean cancelled,
                                          @Nullable Throwable error) {
            if (this.sender != sender) {
                journal("Ignoring endQuery from sender=", sender, "instead of ", this.sender);
                log.warn("Received endQuery() from {}, current sender is {}", sender, this.sender);
                return;
            }
            try {
                TextWebSocketFrame msg = null;
                if (cancelled && error == null && clientCancelled) {
                    (msg = CANCELLED).retain();
                } else if (error != null) {
                    String errMsg = error instanceof NoTraceException
                            ? error.getMessage() : error.toString();
                    msg = new TextWebSocketFrame("!error "+errMsg.replace("\n", "\\n")+"\n");
                }
                if (msg != null) {
                    ctx.write(msg);
                    ctx.writeAndFlush(new CloseWebSocketFrame());
                    ctx.close();
                }
            } finally {
                this.sender = null;
            }
        }

        /* --- --- --- implement WsFrameSender --- --- --- */

        @Override public void sendFrame(ByteBuf content) {
            ctx.writeAndFlush(new TextWebSocketFrame(content));
        }

        @Override public ByteBufSink createSink() {
            return new ByteBufSink(ctx.alloc());
        }

        @Override public ResultsSender<ByteBufSink, ByteBuf> createSender() {
            throw new UnsupportedOperationException();
        }

        /* --- --- --- implement WebSocket extension of SPARQL protocol */

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            SegmentRope msg = bbRopeView.wrapAsSingle(frame.content());
            byte f = msg.len < 2 ? 0 : msg.get(1);
            if (f == 'c' && msg.has(0, CANCEL)) {
                clientCancelled = true;
                endQuery(sender, PARTIAL_CONTENT, true, null);
            } else if (waitingVars) {
                readVarsFrame(msg);
            } else if (bindingsParser != null) {
                readBindings(bindingsParser, msg);
            } else {
                handleQueryCommand(ctx, msg, f);
            }
        }

        private void handleQueryCommand(ChannelHandlerContext ctx, SegmentRope msg, byte f) {
            byte[] ex = null;
            boolean waitingVars = true;
            switch (f) {
                case 'q' -> { bType = BindType.JOIN;       ex = QUERY; waitingVars = false; }
                case 'j' -> { bType = BindType.JOIN;       ex = JOIN; }
                case 'l' -> { bType = BindType.LEFT_JOIN;  ex = LEFT_JOIN; }
                case 'e' -> { bType = BindType.EXISTS;     ex = EXISTS; }
                case 'n' -> { bType = BindType.NOT_EXISTS; ex = NOT_EXISTS; }
                case 'm' -> { bType = BindType.MINUS;      ex = MINUS; }
            }
            this.waitingVars = waitingVars;
            if (ex == null || !msg.has(0, ex)) {
                endQuery(sender, BAD_REQUEST, false,
                        new NoTraceException("Unexpected command: " + msg));
                return;
            }
            var sparql = new ByteRope(msg.toArray(ex.length, msg.len));
            Plan query = parseQuery(sparql);
            if (query == null)
                return;
            if (waitingVars) {
                requestBindingsAt = maxBindings >> 1;
                if (fullBindReq == null) {
                     fullBindReq = ("!bind-request " + maxBindings + "\n").getBytes(UTF_8);
                     halfBindReq = ("!bind-request " + (maxBindings >> 1) + "\n").getBytes(UTF_8);
                }
                ctx.writeAndFlush(new TextWebSocketFrame(
                        ctx.alloc().buffer().writeBytes(fullBindReq)));
            } else {
                var em = sparqlClient.emit(COMPRESSED, query, Vars.EMPTY);
                sender = new WsSender(WsSerializer.create(sizeHint), this, em);
                sender.start();
            }
        }

        private void readBindings(WsServerParser<CompressedBatch> bindingsParser,
                                  SegmentRope msg) {
            try {
                bindingsParser.feedShared(msg);
            } catch (TerminatedException|CancelledException ignored) {
                return;
            }
            if (bindingsParser.rowsParsed() >= requestBindingsAt)
                checkAndRequestBindings();
        }

        private void checkAndRequestBindings() {
            var bindingsParser = this.bindingsParser;
            if (bindingsParser == null)
                return;
            long parsed = bindingsParser.rowsParsed();
            if (parsed >= requestBindingsAt) {
                requestBindingsAt = parsed + (maxBindings>>1);
                var bb = ctx.alloc().buffer(32);
                bb.writeBytes(halfBindReq);
                //noinspection DataFlowIssue bindQuery != null
                long emptySeq = bindQuery.emptySeq;
                WsSender sender = (WsSender)this.sender;
                if (sender != null && sender.canSendEmptyStreak()) {
                    tmpSeq.len = BIND_EMPTY_STREAK.length;
                    tmpSeq.append(emptySeq).append('\n');
                    bb.ensureWritable(tmpSeq.len);
                    bb.writeBytes(tmpSeq.u8(), 0, tmpSeq.len);
                }
                ctx.writeAndFlush(new TextWebSocketFrame(bb));
            }
        }

        private final class BindingsQueue extends CallbackEmitter<CompressedBatch> {
            private @Nullable Runnable resumeTask;

            private BindingsQueue(Vars vars) {
                super(COMPRESSED, vars, EMITTER_SVC, RR_WORKER, CREATED, TASK_FLAGS);
                if (ResultJournal.ENABLED)
                    ResultJournal.initEmitter(this, vars);
            }

            @Override public   String toString() { return ctx.channel().toString(); }
            @Override public   void   pause()    { requestBindingsAt = Long.MAX_VALUE; }

            @Override public void resume() {
                requestBindingsAt = 0;
                var resumeTask = this.resumeTask;
                if (resumeTask == null)
                    this.resumeTask = resumeTask = WsHandler.this::checkAndRequestBindings;
                ctx.executor().execute(resumeTask);
            }

            @Override public void rebind(BatchBinding binding) {
                throw new UnsupportedOperationException();
            }

            @Override public Vars bindableVars() { return Vars.EMPTY; }
        }

        private void readVarsFrame(SegmentRope msg) {
            if (query == null) { // this should be a dead branch
                endQuery(sender, INTERNAL_SERVER_ERROR, false, READ_VARS_NO_QUERY_EX);
                return;
            }
            int len = msg.len, eol = msg.skipUntil(0, len, '\n');
            if (eol == len) {
                endQuery(sender, BAD_REQUEST, false, VARS_NO_LF_EX);
                return;
            }

            // artificially insert BINDING_SEQ_VARNAME as 0-th binding var. WsServerParser
            // will transparently assign values during parsing.
            var bindingsVars = new Vars.Mutable(10);
            bindingsVars.add(WsBindingSeq.VAR);
            for (int i = 0, j; i < eol; i = j+1) {
                byte c = msg.get(i);
                if (c != '?' && c != '$') {
                    endQuery(sender, BAD_REQUEST, false, NO_VAR_MARKER_EX);
                    return;
                }
                j = msg.skipUntil(i, len, '\t',  '\n');
                bindingsVars.add(new ByteRope(j-i-1).append(msg, i+1, j));
            }
            waitingVars = false;

            // create bindings -> BindingStage -> sender pipeline

            var bindingsQueue = new BindingsQueue(bindingsVars);
            bindingsParser = new WsServerParser<>(this, bindingsQueue);
            bindQuery = new VolatileBindQuery(query, bindingsQueue, bType);
            var results = sparqlClient.emit(bindQuery, Vars.EMPTY);
            var sender = new WsSender(WsSerializer.create(sizeHint), this, results);
            this.sender = sender;

            // only send binding seq number and right unbound vars
            Vars all = results.vars();
            var serializeVars = new Vars.Mutable(all.size()+1);
            serializeVars.add(WsBindingSeq.VAR);
            for (var v : all)
                if (!bindingsVars.contains(v)) serializeVars.add(v);
            sender.serializeVars = serializeVars;

            // start bindings -> BindingStage -> sender pipiline
            sender.start();
            readBindings(bindingsParser, msg);
        }

    }

}

