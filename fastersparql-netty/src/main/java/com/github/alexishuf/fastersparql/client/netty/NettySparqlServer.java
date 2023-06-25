package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.BindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.netty.util.ByteBufRopeView;
import com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink;
import com.github.alexishuf.fastersparql.client.netty.util.NettyResultsSender;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRopeUtils;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
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
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.WsBindingSeq;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.WsServerParserBIt;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
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

import static com.github.alexishuf.fastersparql.FSProperties.wsServerBindings;
import static com.github.alexishuf.fastersparql.batch.type.Batch.COMPRESSED;
import static com.github.alexishuf.fastersparql.util.UriUtils.unescape;
import static com.github.alexishuf.fastersparql.util.UriUtils.unescapeToRope;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED;
import static io.netty.handler.codec.http.HttpHeaderValues.CHUNKED;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.util.AsciiString.indexOfIgnoreCaseAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Expose a {@link SparqlClient} (which includes a {@link Federation}) through the SPARQL
 * protocol (HTTP) and the custom WebSocket protocol.
 */
public class NettySparqlServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NettySparqlServer.class);
    private static final String SP_PATH = "/sparql";
    private static final String APPLICATION_SPARQL_QUERY = "application/sparql-query";
    private static final String TEXT_PLAIN_U8 = "text/plain; charset=utf-8";

    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup workerGroup;
    private final Channel server;
    private final SparqlClient sparqlClient;
    private final ContentNegotiator negotiator = ResultsSerializer.contentNegotiator();
    private final String noSerializerError = "Cannot generate any of the formats in this request" +
            " \"Accepts\" header. Supported formats: "+ Arrays.stream(negotiator.supported())
                                                              .map(MediaType::toString)
                                                              .collect(Collectors.joining(", "));
    private final FSCancelledException wsCancelledEx;
    private int serializeSizeHint = WsSerializer.DEF_BUFFER_HINT;

    public NettySparqlServer(SparqlClient sparqlClient, String host, int port) {
        this.sparqlClient = sparqlClient;
        wsCancelledEx = new FSCancelledException(sparqlClient.endpoint(), "!cancel frame received");
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
                                .addLast("ws-sparql", new WsSparqlHandler());
                    }
                }).bind(host, port).syncUninterruptibly().channel();
    }

    /** Get the TCP port where the server is listening to connections. */
    public int port() {
        return ((InetSocketAddress)server.localAddress()).getPort();
    }

    /** Get the {@link InetSocketAddress} where the server is listening for connections. */
    public InetSocketAddress listenAddress() {
        return (InetSocketAddress)server.localAddress();
    }

    @Override public String toString() {
        return "NettySparqlServer"+server+"("+sparqlClient.endpoint()+")";
    }

    @Override public void close()  {
        sparqlClient.close();
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
        }
    }

    /* --- --- --- handlers --- --- --- */

    private static class VolatileBindQuery extends BindQuery<CompressedBatch> {
        volatile long nonEmptySeq = -1, emptySeq = -1;

        public VolatileBindQuery(SparqlQuery query, BIt<CompressedBatch> bindings, BindType type) {
            super(query, bindings, type);
        }

        @Override public void    emptyBinding(long sequence) {    emptySeq = sequence; }
        @Override public void nonEmptyBinding(long sequence) { nonEmptySeq = sequence; }
    }

    private abstract class QueryHandler<T> extends SimpleChannelInboundHandler<T> {
        protected @MonotonicNonNull ChannelHandlerContext ctx;
        protected @Nullable Plan query;
        protected final SparqlParser sparqlParser = new SparqlParser();
        protected BIt<CompressedBatch> it;
        protected @Nullable VolatileBindQuery bindQuery;
        protected ByteBufRopeView bbRopeView = ByteBufRopeView.create();
        protected Vars serializeVars = Vars.EMPTY;
        @MonotonicNonNull protected ResultsSerializer serializer;
        protected int round = -1;

        @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            if (this.ctx == ctx) return;
            this.ctx = ctx;
            super.channelRegistered(ctx);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            if (it != null)
                fail(ctx.channel() + " closed before query completed");
        }

        @Override public void channelUnregistered(ChannelHandlerContext ctx) {
            bbRopeView.recycle();
            bbRopeView = null;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            endRound(INTERNAL_SERVER_ERROR, cause.getMessage(), cause);
        }

        @Override public String toString() {
            String simpleName = getClass().getSimpleName();
            if (ctx == null)
                return "Unregistered "+simpleName;
            return simpleName+ctx.channel()+'('+sparqlClient.endpoint().uri()+')';
        }

        /**
         * Called once when the current query processing round is complete.
         *
         * <p>Implementations of this method MUST signal the completion to clients, either by
         * sending some data or by closing the connection.</p>
         *
         * @param status HTTP status to send if the client expects a {@link HttpResponse}
         * @param errorMessage If non-null this is a message that describes the error and is
         *                     intended to be forwarded to the client. If null, it signals the
         *                     query completed normally (all result rows have been already
         *                     serialized to the client).
         * @param cancelled if true, this signals the query was cancelled before completion.
         *                  This being {@code true} implies {@code errorMessage} being non-null.
         */
        protected abstract void onFailure(HttpResponseStatus status,
                                          CharSequence errorMessage,
                                          boolean cancelled);

        /**
         * This method will be called once per round, {@link #endRound(HttpResponseStatus, CharSequence, Throwable)}
         * effectively ends the round. This method will be called even on successful round
         * completions (when {@link #onFailure(HttpResponseStatus, CharSequence, boolean)}
         * is not called).
         *
         * <p>Implementations should stop still running background tasks and release resources
         * specific to the round that just ended. If
         * {@link #onFailure(HttpResponseStatus, CharSequence, boolean)} is called, this will run
         * after its return.</p>
         */
        public void cleanup() {
            if (it != null) {
                try {
                    it.close();
                } catch (Throwable t) {
                    log.error("Ignoring {} while closing {}", t, it);
                } finally {
                    this.it = null;
                }
            }
            this.query = null;
        }

        protected int beginRound() {
            if (round > 0)
                throw new IllegalStateException("Starting new round before last one completed");
            return round = -round+1;
        }

        /**
         * Ends current query processing round, closing the {@link BIt}, its associated
         * drainer thread and sending a response to the client.
         *
         * @param status forwarded to {@link #onFailure(HttpResponseStatus, CharSequence, boolean)}
         * @param errorMsg An error message forwarded to {@link #onFailure(HttpResponseStatus, CharSequence, boolean)}
         *                 if this is {@code null} but {@code cause} is not,
         *                 {@code cause.getMessage()} will be used
         * @param cause If non-null a stack trace will be appended to the
         *              {@link #onFailure(HttpResponseStatus, CharSequence, boolean)} error message.
         * @return {@code true} if the current round was still {@code round} and it was completed
         *         by this call
         */
        protected final boolean endRound(HttpResponseStatus status,
                                         @Nullable CharSequence errorMsg,
                                         @Nullable Throwable cause) {
            assert ctx == null || ctx.executor().inEventLoop();
            int round = this.round;
            if (round <= 0)
                return false; // already ended
            try {
                boolean cancelled = (cause instanceof FSCancelledException ce
                                        && ce.endpoint() == sparqlClient.endpoint())
                                 || (cause instanceof BItReadClosedException re && re.it() == it);
                ByteRope msg;
                if (cause == null && errorMsg == null) {
                    msg = null;
                } else {
                    msg = new ByteRope();
                    if      (errorMsg != null) msg.append(errorMsg);
                    else                       msg.append("Could not complete query: ")
                                                  .append(cause.getMessage());
                    if (query != null) {
                        msg.append("\nQuery:\n").indented(2, query.sparql());
                        if (msg.get(msg.len-1) != '\n') msg.append('\n');
                    }
                    if (cause != null) {
                        try (var w = new PrintStream(msg.asOutputStream(), true, UTF_8)) {
                            cause.printStackTrace(w);
                        }
                    }
                }
                if (status != OK || cause != null || errorMsg != null)
                    onFailure(status, msg, cancelled);
                cleanup();
                return true; // this call ended round
            } finally {
                this.round = -round;
            }
        }

        protected boolean fail(CharSequence errorMessage) {
            endRound(INTERNAL_SERVER_ERROR, errorMessage, null);
            return true;
        }

        protected boolean parseQuery(SegmentRope sparql) {
            try {
                query = sparqlParser.parse(sparql, 0);
                return true;
            } catch (Throwable e) {
                endRound(BAD_REQUEST, "Bad query syntax", e);
            }
            return false;
        }

        protected boolean dispatchQuery(@Nullable BIt<CompressedBatch> bindings,
                                        BindType type) {
            try {
                if (bindings != null) {
                    bindQuery = new VolatileBindQuery(query, bindings, type);
                    it = sparqlClient.query(bindQuery);
                } else {
                    bindQuery = null;
                    it = sparqlClient.query(COMPRESSED, query);
                }
                serializeVars = it.vars();
                return true;
            } catch (Throwable t) {
                endRound(INTERNAL_SERVER_ERROR, "Could not dispatch query", t);
            }
            return false;
        }

        private static final class RoundEndedException extends RuntimeException {
            public static final RoundEndedException INSTANCE = new RoundEndedException();
        }

        protected abstract class SerializeTask<M> extends NettyResultsSender<M> {
            protected final int drainerRound;
            private boolean ended;

            protected final class EndAction extends NettyResultsSender.Action {
                private final M msg;
                public EndAction(M msg) {
                    super("END");
                    this.msg = msg;
                }
                @Override public void run(NettyResultsSender<?> sender) { //runs on event loop
                    if (ended) return;
                    ended = true;
                    endRound(OK, null, null);
                    ctx.writeAndFlush(msg);
                }
            }

            public SerializeTask(int round) {
                super(QueryHandler.this.serializer, ctx);
                sink.sizeHint(serializeSizeHint);
                this.drainerRound = round;
            }

            @Override protected void beforeSend() { // runs on event loop
                if (drainerRound != round)
                    throw RoundEndedException.INSTANCE;
            }

            @Override protected void onError(Throwable t) { // runs on event loop
                if (ended || t == RoundEndedException.INSTANCE)
                    return;
                ended = true;
                endRound(INTERNAL_SERVER_ERROR, null, t);
            }

            @Override public void sendTrailer() { // runs on drainerThread
                if (sink.needsTouch()) touch();
                serializeSizeHint = ByteBufSink.adjustSizeHint(serializeSizeHint, sink.sizeHint());
                try {
                    serializer.serializeTrailer(sink);
                    execute(new EndAction(wrapLast(sink.take())));
                } catch (Throwable t) {
                    execute(t);
                }
            }

            @Override public String toString() {
                return "SerializeTask{round="+round+", handler="+QueryHandler.this+'}';
            }
        }

        protected abstract SerializeTask<?> createSerializeTask(int round);

        protected void drainerThread(int round) {
            SerializeTask<?> task = null;
            try (var it = this.it) {
                task = createSerializeTask(round);
                //Thread.currentThread().setName("drainer-" + ctx.channel());
                Thread.currentThread().setName("drainer");
                if (query == null || it == null) {
                    fail("null query or it");
                    return;
                }
                task.sendInit(it.vars(), serializeVars, query.isAsk());
                for (CompressedBatch b = null; (b = it.nextBatch(b)) != null; ) {
                    task.sendSerialized(b);
                }
                task.sendTrailer();
            } catch (NettyResultsSender.NettyExecutionException e) {
                log.debug("Drainer thread exiting", e);
            } catch (Throwable t) {
                if (task != null) {
                    log.info("Drainer thread exiting due to {}", t.toString());
                    task.sendError(t);
                } else {
                    log.error("Error creating SerializeTask, racing to end round", t);
                    endRound(INTERNAL_SERVER_ERROR, null, t);
                }
            } finally {
                if (task != null)
                    task.close();
            }
        }
    }

    private final class SparqlHandler extends QueryHandler<FullHttpRequest> {
        private @MonotonicNonNull HttpVersion httpVersion;
        private boolean responseStarted = false;

        private static final Pattern QUERY_RX = Pattern.compile("(?i)[?&]query=([^&]+)");
        private static final byte[] QUERY_EQ = "QUERY=".getBytes(UTF_8);

        /* --- --- --- QueryHandler methods --- --- --- */

        private final class SparqlSerializeTask extends SerializeTask<HttpContent> {
            public SparqlSerializeTask(int drainerRound) {super(drainerRound);}
            @Override protected HttpContent wrap(ByteBuf bb) {
                return new DefaultHttpContent(bb);
            }
            @Override protected HttpContent wrapLast(ByteBuf bb) {
                return new DefaultLastHttpContent(bb);
            }
            @Override public void sendCancel() {
                sendError(new FSCancelledException(sparqlClient.endpoint()));
            }
        }

        @Override
        protected SerializeTask<?> createSerializeTask(int round) {
            return new SparqlSerializeTask(round);
        }

        @Override protected void onFailure(HttpResponseStatus status, CharSequence errorMessage,
                                           boolean cancelled) {
            HttpContent msg;
            ByteBuf bb = ctx.alloc().buffer(errorMessage.length());
            if (responseStarted) {
                msg = new DefaultHttpContent(NettyRopeUtils.write(bb, errorMessage));
            } else {
                NettyRopeUtils.write(bb, errorMessage);
                var res = new DefaultFullHttpResponse(httpVersion, status, bb);
                res.headers().set(CONTENT_TYPE, TEXT_PLAIN_U8)
                             .set(CONTENT_LENGTH, bb.readableBytes());
                msg = res;
            }
            ctx.writeAndFlush(msg);
        }

        /* --- --- --- SimpleChannelInboundHandler methods and request handling --- --- --- */

        @Override protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            if (handleUpgrade(req))
                return;
            httpVersion = req.protocolVersion();
            responseStarted = false;
            int r = beginRound();
            if (!req.decoderResult().isSuccess()) {
                endRound(BAD_REQUEST, "Could not decode HTTP request", null);
            } else if (!req.uri().startsWith(SP_PATH)) {
                endRound(NOT_FOUND, "Path not found, HTTP sparql endpoint is at SP_PATH", null);
            } else if (badMethod(req.method())) {
                endRound(METHOD_NOT_ALLOWED, "Only GET and POST are allowed", null);
            } else if (chooseSerializer(req)) {
                if (req.method().equals(HttpMethod.HEAD)) {
                    handleHead(req);
                } else if (req.method().equals(HttpMethod.GET)) {
                    handleGet(r, req);
                } else if (req.method().equals(HttpMethod.POST)) {
                    handlePost(r, req);
                }
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
                beginRound();
                endRound(BAD_REQUEST, "Only possible upgrade is websocket", null);
            }
            return true; // "handled" the request
        }

        private void handleHead(HttpRequest req) {
            var resp = new DefaultFullHttpResponse(req.protocolVersion(), OK);
            resp.headers().set(CONTENT_TYPE, serializer.contentType())
                          .set(CONTENT_LENGTH, 0)
                          .set(ALLOW, "GET, POST, HEAD");
            ctx.writeAndFlush(resp);
            endRound(OK, null, null);
        }

        private void handleGet(int round, HttpRequest req) {
            String uri = req.uri();
            var m = QUERY_RX.matcher(uri);
            if (!m.find()) {
                endRound(BAD_REQUEST, "Missing query parameter in GET", null);
                return;
            }
            handleQuery(round, unescapeToRope(m.group(1)));
        }

        private void handlePost(int round, FullHttpRequest req) {
            String ct = req.headers().get(CONTENT_TYPE);
            PlainRope body = bbRopeView.wrap(req.content());
            if (indexOfIgnoreCaseAscii(ct, APPLICATION_X_WWW_FORM_URLENCODED, 0) == 0) {
                int begin = 0, len = body.len;
                while (begin < len && !body.hasAnyCase(begin, QUERY_EQ))
                    begin = body.skipUntil(begin, len, '&')+1;
                begin += QUERY_EQ.length;
                if (begin >= len)
                    endRound(BAD_REQUEST, "No query in form",  null);
                else
                    handleQuery(round, unescape(body, begin, body.skipUntil(begin, body.len, '&')));
            } else if (indexOfIgnoreCaseAscii(ct, APPLICATION_SPARQL_QUERY, 0) == 0) {
                handleQuery(round, new ByteRope(body));
            } else {
                endRound(UNSUPPORTED_MEDIA_TYPE, "Expected Content-Type to be application/x-www-form-urlencoded or application/sparql-query, got "+ct, null);
            }
        }

        private void handleQuery(int round, SegmentRope sparqlRope) {
            if (parseQuery(sparqlRope) && dispatchQuery(null, BindType.JOIN)) {
                if (httpVersion == HttpVersion.HTTP_1_0) {
                    handleQueryHttp10(round);
                } else {
                    var res = new DefaultHttpResponse(httpVersion, OK);
                    res.headers().set(CONTENT_TYPE, serializer.contentType())
                                 .set(TRANSFER_ENCODING, CHUNKED);
                    responseStarted = true;
                    ctx.writeAndFlush(res);
                    Thread.startVirtualThread(() -> drainerThread(round));
                }
            }
        }

        private void handleQueryHttp10(int round) {
            Thread.startVirtualThread(() -> handleQueryHttp10Thread(round));
        }

        private void handleQueryHttp10Thread(int round) {
            var it = this.it;
            var query = this.query;
            if (it == null || query == null) {
                fail("null query or it");
                return;
            }
            var sink = new ByteBufSink(ctx.alloc()).touch();
            try {
                serializer.init(it.vars(), it.vars(), query.isAsk(), sink);
                for (CompressedBatch b = null; (b = it.nextBatch(b)) != null; ) {
                    if (this.round != round) return;
                    serializer.serialize(b, sink);
                }
                serializer.serializeTrailer(sink);
                ByteBuf bb = sink.take();
                var res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, OK, bb);
                res.headers().set(CONTENT_TYPE, serializer.contentType())
                             .set(CONTENT_LENGTH, bb.readableBytes());
                if (endRound(OK, null, null))
                    ctx.writeAndFlush(res);
                else
                    res.release();
            } catch (Throwable t) {
                endRound(INTERNAL_SERVER_ERROR, null, t);
                throw t;
            } finally {
                sink.release();
            }
        }

        private static boolean badMethod(HttpMethod method) {
            return !method.equals(HttpMethod.GET) && !method.equals(HttpMethod.HEAD)
                    && !method.equals(HttpMethod.POST);
        }

        private boolean chooseSerializer(HttpRequest req) {
            try {
                MediaType mt = negotiator.select(req.headers().valueStringIterator(ACCEPT));
                if (mt != null) {
                    serializer = ResultsSerializer.create(mt);
                    return true;
                }
            } catch (Throwable t) {
                log.error("Failed to select and create serializer for accept string {}",
                          req.headers().getAllAsString(ACCEPT), t);
            }
            endRound(NOT_ACCEPTABLE, noSerializerError, null);
            return false;
        }
    }

    private final class WsSparqlHandler extends QueryHandler<WebSocketFrame>
            implements WsFrameSender<ByteBufSink, ByteBuf> {
        private static final byte[] QUERY = "!query".getBytes(UTF_8);
        private static final byte[] JOIN = "!join".getBytes(UTF_8);
        private static final byte[] LEFT_JOIN = "!left-join".getBytes(UTF_8);
        private static final byte[] EXISTS = "!exists".getBytes(UTF_8);
        private static final byte[] MINUS = "!minus".getBytes(UTF_8);
        private static final byte[] NOT_EXISTS = "!not-exists".getBytes(UTF_8);
        private static final byte[] CANCEL = "!cancel".getBytes(UTF_8);
        private static final byte[] ERROR = "!error ".getBytes(UTF_8);
        private static final byte[] CANCELLED = "!cancelled ".getBytes(UTF_8);
        private static final byte[] BIND_EMPTY_STREAK = "!bind-empty-streak ".getBytes(UTF_8);
        private static final ByteBuf CANCELLED_BB = Unpooled.copiedBuffer("!cancelled unknown reason\n", UTF_8);

        private final int maxBindings = wsServerBindings();

        private @Nullable WsServerParserBIt<CompressedBatch> bindingsParser;
        private byte @MonotonicNonNull [] fullBindReq, halfBindReq;
        private int requestBindingsAt;
        private int waitingVarsRound;
        private BindType bType = BindType.JOIN;
        private WsSerializeTask serializeTask;
        private final SegmentRope tmpView = new SegmentRope();
        private final ByteRope tmpSeq = new ByteRope(BIND_EMPTY_STREAK.length+12).append(BIND_EMPTY_STREAK);

        public WsSparqlHandler() {
            serializer = WsSerializer.create(serializeSizeHint);
        }

        /* --- --- --- QueryHandler methods --- --- --- */

        @Override
        protected void onFailure(HttpResponseStatus status, CharSequence errorMessage,
                                 boolean cancelled) {
            var sink = new ByteBufSink(ctx.alloc()).touch();
            sink.append(cancelled ? CANCELLED : ERROR).appendEscapingLF(errorMessage).append('\n');
            ctx.writeAndFlush(new TextWebSocketFrame(sink.take()));
        }

        @Override public void cleanup() {
            super.cleanup();
            waitingVarsRound = 0;
            try {
                var bindingsParser = this.bindingsParser;
                if (bindingsParser != null)
                    bindingsParser.close();
            } catch (Throwable t) {
                log.error("Ignoring {} while closing bindingsParser={}", t, bindingsParser);
            } finally { this.bindingsParser = null; }
        }

        private final class WsSerializeTask extends SerializeTask<TextWebSocketFrame> {
            private volatile long lastSentSeq = -1;

            public WsSerializeTask(int drainerRound) {
                super(drainerRound);
                if (drainerRound == round)
                    serializeTask = this;
            }

            @Override protected TextWebSocketFrame wrap(ByteBuf bb) {
                return new TextWebSocketFrame(bb);
            }

            boolean canSendEmptyStreak() { // runs on event loop
                lock(); // required due to race on this.active
                try {
                    if (active || bindQuery == null) return false;
                    long lastSent = lastSentSeq;
                    return lastSent == bindQuery.nonEmptySeq && bindQuery.emptySeq > lastSent;
                } finally { unlock(); }
            }

            @Override public void sendSerialized(Batch<?> batch) { //runs on drainerThread
                super.sendSerialized(batch);
                if (bindQuery == null || batch.rows == 0)
                    return;
                if (!batch.localView(batch.rows - 1, 0, tmpView))
                    throw new IllegalStateException("Missing binding sequence");
                lastSentSeq = WsBindingSeq.parse(tmpView, 0, tmpView.len);
            }

            @Override public void sendCancel() { execute(CANCELLED_BB); }
        }

        @Override
        protected SerializeTask<?> createSerializeTask(int round) {
            return new WsSerializeTask(round);
        }

        /* --- --- --- SimpleChannelInboundHandler methods and request handling --- --- --- */

        @Override public void channelUnregistered(ChannelHandlerContext ctx) {
            super.channelUnregistered(ctx);
            ((WsSerializer)serializer).recycle();
            serializer = null;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) {
            SegmentRope msg = bbRopeView.wrapAsSingle(frame.content());
            byte f = msg.len < 2 ? 0 : msg.get(1);
            if (f == 'c' && msg.has(0, CANCEL)) {
                endRound(INTERNAL_SERVER_ERROR, null, wsCancelledEx);
            } else if (waitingVarsRound > 0) {
                readVarsFrame(msg);
            } else if (bindingsParser != null) {
                readBindings(bindingsParser, msg);
            } else {
                handleQueryCommand(ctx, msg, f);
            }
        }

        private void handleQueryCommand(ChannelHandlerContext ctx, SegmentRope msg, byte f) {
            byte[] ex = null;
            int round = beginRound();
            waitingVarsRound = round;
            switch (f) {
                case 'q' -> { bType = BindType.JOIN;       ex = QUERY; waitingVarsRound = 0; }
                case 'j' -> { bType = BindType.JOIN;       ex = JOIN; }
                case 'l' -> { bType = BindType.LEFT_JOIN;  ex = LEFT_JOIN; }
                case 'e' -> { bType = BindType.EXISTS;     ex = EXISTS; }
                case 'n' -> { bType = BindType.NOT_EXISTS; ex = NOT_EXISTS; }
                case 'm' -> { bType = BindType.MINUS;      ex = MINUS; }
            }
            if (ex == null || !msg.has(0, ex)) {
                fail(new ByteRope().append("Unexpected frame: ").appendEscapingLF(msg));
            } else {
                var sparql = new ByteRope(msg.toArray(ex.length, msg.len));
                if (parseQuery(sparql)) {
                    if (waitingVarsRound > 0) {
                        requestBindingsAt = maxBindings >> 1;
                        if (fullBindReq == null) {
                            fullBindReq = ("!bind-request " + maxBindings + "\n").getBytes(UTF_8);
                            halfBindReq = ("!bind-request " + (maxBindings >> 1) + "\n").getBytes(UTF_8);
                        }
                        ctx.writeAndFlush(new TextWebSocketFrame(
                                ctx.alloc().buffer().writeBytes(fullBindReq)));
                    } else {
                        if (dispatchQuery(null, BindType.JOIN)) {
                            Thread.startVirtualThread(() -> drainerThread(round));
                        }
                    }
                }
            }
        }

        private void readBindings(WsServerParserBIt<CompressedBatch> bindingsParser,
                                  SegmentRope msg) {
            bindingsParser.feedShared(msg);
            if (bindingsParser.rowsParsed() >= requestBindingsAt) {
                requestBindingsAt += maxBindings>>1;
                var bb = ctx.alloc().buffer(32);
                bb.writeBytes(halfBindReq);
                //noinspection DataFlowIssue bindQuery != null
                long emptySeq = bindQuery.emptySeq;
                if (serializeTask != null && serializeTask.canSendEmptyStreak()) {
                    tmpSeq.len = BIND_EMPTY_STREAK.length;
                    tmpSeq.append(emptySeq).append('\n');
                    bb.ensureWritable(tmpSeq.len);
                    bb.writeBytes(tmpSeq.u8(), 0, tmpSeq.len);
                }
                ctx.writeAndFlush(new TextWebSocketFrame(bb));
            }
        }

        private void readVarsFrame(SegmentRope msg) {
            if (query == null && fail("null query")) return;
            int len = msg.len, eol = msg.skipUntil(0, len, '\n');
            if (eol == len && fail("No LF (\\n) in vars after !bind frame"))
                return;

            // artificially insert BINDING_SEQ_VARNAME as 0-th binding var. WsServerParserBIt
            // will transparently assign values during parsing.
            var bindingsVars = new Vars.Mutable(10);
            bindingsVars.add(WsBindingSeq.VAR);
            for (int i = 0, j; i < eol; i = j+1) {
                byte c = msg.get(i);
                if (c != '?' && c != '$' && fail("Missing ?/$ in var name"))
                    return;
                j = msg.skipUntil(i, len, '\t',  '\n');
                bindingsVars.add(new ByteRope(j-i-1).append(msg, i+1, j));
            }
            bindingsParser = new WsServerParserBIt<>(this, COMPRESSED, bindingsVars,
                                                     wsServerBindings());
            // do not block if client starts flooding
            bindingsParser.maxReadyItems(Integer.MAX_VALUE);
            int round = waitingVarsRound;
            waitingVarsRound = 0;

            if (dispatchQuery(bindingsParser, bType)) {
                // only send binding seq number and right unbound vars
                Vars itVars = it.vars();
                var serializeVars = new Vars.Mutable(itVars.size()+1);
                serializeVars.add(WsBindingSeq.VAR);
                for (var v : itVars)
                    if (!bindingsVars.contains(v)) serializeVars.add(v);
                this.serializeVars = serializeVars;
                Thread.startVirtualThread(() -> drainerThread(round));
                readBindings(bindingsParser, msg);
            }
        }

        /* --- --- --- WsFrameSender --- --- --- */

        @Override public void sendFrame(ByteBuf content) {
            ctx.writeAndFlush(new TextWebSocketFrame(content));
        }
        @Override public ByteBufSink createSink() {
            return new ByteBufSink(ctx.alloc());
        }
        @Override public ResultsSender<ByteBufSink, ByteBuf> createSender() {
            return new WsSerializeTask(round);
        }
    }
}
