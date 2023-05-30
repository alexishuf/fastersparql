package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRopeUtils;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.fed.Federation;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.ContentNegotiator;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.parser.SparqlParser;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.WsServerParserBIt;
import com.github.alexishuf.fastersparql.sparql.results.serializer.ResultsSerializer;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
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
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
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
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.charset.StandardCharsets.UTF_8;

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

    public NettySparqlServer(SparqlClient sparqlClient, String host, int port) {
        this.sparqlClient = sparqlClient;
        wsCancelledEx = new FSCancelledException(sparqlClient.endpoint(), "!cancel frame received");
        acceptGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        server = new ServerBootstrap().group(acceptGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
//                .handler(new LoggingHandler(NettySparqlServer.class, LogLevel.INFO, ByteBufFormat.SIMPLE))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("http", new HttpServerCodec())
//                                .addLast("log", new LoggingHandler(NettySparqlServer.class, LogLevel.INFO, ByteBufFormat.SIMPLE))
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
        server.close().addListener(f -> {
            acceptGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        });
    }

    /* --- --- --- handlers --- --- --- */

    private abstract class QueryHandler<T> extends SimpleChannelInboundHandler<T> {
        private static final VarHandle ROUND, DRAINER_WRITING;
        static {
            try {
                ROUND = lookup().findVarHandle(QueryHandler.class, "plainRound", int.class);
                DRAINER_WRITING = lookup().findVarHandle(QueryHandler.class, "plainDrainerWriting", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        protected @MonotonicNonNull ChannelHandlerContext ctx;
        protected @Nullable Plan query;
        protected BIt<CompressedBatch> it;
        @MonotonicNonNull protected ResultsSerializer serializer;
        @SuppressWarnings({"FieldMayBeFinal", "unused"}) // access through ROUND/DRAINER_WRITING
        private int plainRound = -1, plainDrainerWriting;

        @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
            this.ctx = ctx;
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            if (it != null)
                fail(ctx.channel() + " closed before query completed");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            endRound(0, INTERNAL_SERVER_ERROR, cause.getMessage(), cause);
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
         * This method will be called once per round, {@link #endRound(int, HttpResponseStatus, CharSequence, Throwable)}
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
            int round;
            boolean blocked = false;
            try {
                // it may be that a endRound() from drainerThread() is ongoing. Before we start
                // a new round we must wait until endRound(), onFailure() and cleanup() finish
                // these methods do not block but are not trivial, thus we yield() instead of
                // onSpinWait()
                while (true) {
                    round = (int) ROUND.getOpaque(this);
                    if (round == 0) {
                        blocked = true;
                        LockSupport.setCurrentBlocker(this);
                        Thread.yield();
                    } else if (round < 0) {
                        int ex = round;
                        if (ROUND.compareAndSet(this, ex, round = -round + 1))
                            break;
                    } else {
                        throw new IllegalStateException("Starting new round before last one completed");
                    }
                }
            } finally {
                if (blocked) LockSupport.setCurrentBlocker(null);
            }

            return round;
        }

        protected boolean atRound(int round) { return (int)ROUND.getOpaque(this) == round; }

        /**
         * Ends current query processing round, closing the {@link BIt}, its associated
         * drainer thread and sending a response to the client.
         *
         * @param status forwarded to {@link #onFailure(HttpResponseStatus, CharSequence, boolean)}
         * @param round The round to end, returned by {@link #beginRound()}. If zero,
         *              will use the current round. Only threads running at the Netty event loop
         *              should pass zero to this argument. Other background threads must pass the
         *              round number they got (indirectly) from {@link #beginRound()} since
         *              they may race with other {@link #beginRound()} calls and end the
         *              round after the one they intended.
         * @param errorMsg An error message forwarded to {@link #onFailure(HttpResponseStatus, CharSequence, boolean)}
         *                 if this is {@code null} but {@code cause} is not,
         *                 {@code cause.getMessage()} will be used
         * @param cause If non-null a stack trace will be appended to the
         *              {@link #onFailure(HttpResponseStatus, CharSequence, boolean)} error message.
         * @return {@code true} if the current round was still {@code round} and it was completed
         *         by this call
         */
        protected final boolean endRound(int round, HttpResponseStatus status,
                                   @Nullable CharSequence errorMsg, @Nullable Throwable cause) {
            if (round == 0)
                round = (int) ROUND.getOpaque(this);
            if (round <= 0 || !ROUND.compareAndSet(this, round, 0))
                return false; // already ended
            try {
                // wait until drainer finishes queueing its HttpObject
                // since we already changed ROUND, we are unlikely to spin here for long
                while ((int)DRAINER_WRITING.getAcquire(this) == 1) Thread.onSpinWait();
                // from this point onwards drainerThread() will not send any more messages
                boolean cancelled;
                if (cause instanceof FSCancelledException e)
                    cancelled = e.endpoint() == sparqlClient.endpoint();
                else if (cause instanceof BItReadClosedException e)
                    cancelled = e.it() == this.it;
                else
                    cancelled = false;
                ByteRope msg;
                if (cause == null && errorMsg == null) {
                    msg = null;
                } else {
                    msg = new ByteRope();
                    if      (errorMsg != null) msg.append(errorMsg);
                    else                       msg.append("Could not complete query: ")
                                                  .append(cause.getMessage());
                    if (query != null)
                        msg.append("\nQuery:\n").indented(2, query.sparql());
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
                ROUND.setRelease(this, -round);
            }
        }

        protected boolean fail(CharSequence errorMessage) {
            endRound(0, INTERNAL_SERVER_ERROR, errorMessage, null);
            return true;
        }

        protected boolean parseQuery(SegmentRope sparql, int start) {
            try {
                query = new SparqlParser().parse(sparql, start);
                return true;
            } catch (Throwable e) {
                endRound(0, BAD_REQUEST, "Bad query syntax", e);
            }
            return false;
        }

        protected boolean dispatchQuery(@Nullable BIt<CompressedBatch> bindings, BindType type) {
            try {
                if (bindings != null)
                    it = sparqlClient.query(COMPRESSED, query, bindings, type);
                else
                    it = sparqlClient.query(COMPRESSED, query);
                return true;
            } catch (Throwable t) {
                endRound(0, INTERNAL_SERVER_ERROR, "Could not dispatch query", t);
            }
            return false;
        }


        protected abstract void sendChunk(ByteBuf bb);
        protected void sendLastChunk(ByteBuf bb) { sendChunk(bb); }

        protected void drainerThread(int round) {
            Thread.currentThread().setName("drainer-"+ctx.channel());
            var it  = this.it;
            var query = this.query;
            if (query == null || it == null) {
                fail("null query or it");
                return;
            }
            var sink = new ByteBufSink(ctx.alloc());
            try {
                serializer.init(it.vars(), it.vars(), query.isAsk(), sink.touch());
                for (CompressedBatch b = null; (b = it.nextBatch(b)) != null; ) {
                    serializer.serialize(b, sink.touch());
                    ByteBuf bb = sink.take();
                    DRAINER_WRITING.setRelease(this, 1);
                    try {
                        if ((int) ROUND.getAcquire(this) != round) {
                            bb.release();
                            return;
                        }
                        sendChunk(bb);
                    } finally { DRAINER_WRITING.setRelease(this, 0); }
                }
                serializer.serializeTrailer(sink.touch());
                if (endRound(round, OK, null, null))
                    sendLastChunk(sink.take());
            } catch (Throwable t) {
                endRound(round, INTERNAL_SERVER_ERROR, null, t);
                throw t;
            } finally {
                sink.release();
            }
        }
    }

    private final class SparqlHandler extends QueryHandler<FullHttpRequest> {
        private @MonotonicNonNull HttpVersion httpVersion;
        private boolean responseStarted = false;

        private static final Pattern QUERY_RX = Pattern.compile("(?i)[?&]query=([^&]+)");
        private static final byte[] QUERY_EQ = "QUERY=".getBytes(UTF_8);

        /* --- --- --- QueryHandler methods --- --- --- */

        @Override protected void sendChunk(ByteBuf bb) {
            ctx.writeAndFlush(new DefaultHttpContent(bb));
        }

        @Override protected void sendLastChunk(ByteBuf bb) {
            ctx.writeAndFlush(new DefaultLastHttpContent(bb));
        }

        @Override protected void onFailure(HttpResponseStatus status, CharSequence errorMessage,
                                           boolean cancelled) {
            HttpContent msg;
            if (responseStarted) {
                ByteBuf bb = ctx.alloc().buffer(errorMessage.length());
                msg = new DefaultHttpContent(NettyRopeUtils.write(bb, errorMessage));
            } else {
                ByteBuf bb = ctx.alloc().buffer(errorMessage.length());
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
                endRound(r, BAD_REQUEST, "Could not decode HTTP request", null);
            } else if (!req.uri().startsWith(SP_PATH)) {
                endRound(r, NOT_FOUND, "Path not found, HTTP sparql endpoint is at SP_PATH", null);
            } else if (badMethod(req.method())) {
                endRound(r, METHOD_NOT_ALLOWED, "Only GET and POST are allowed", null);
            } else if (chooseSerializer(req)) {
                if (req.method().equals(HttpMethod.HEAD)) {
                    handleHead(r, req);
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
                endRound(beginRound(), BAD_REQUEST, "Only possible upgrade is websocket", null);
            }
            return true; // "handled" the request
        }

        private void handleHead(int round, HttpRequest req) {
            var resp = new DefaultFullHttpResponse(req.protocolVersion(), OK);
            resp.headers().set(CONTENT_TYPE, serializer.contentType())
                          .set(CONTENT_LENGTH, 0)
                          .set(ALLOW, "GET, POST, HEAD");
            ctx.writeAndFlush(resp);
            endRound(round, OK, null, null);
        }

        private void handleGet(int round, HttpRequest req) {
            String uri = req.uri();
            var m = QUERY_RX.matcher(uri);
            if (!m.find()) {
                endRound(0, BAD_REQUEST, "Missing query parameter in GET", null);
                return;
            }
            handleQuery(round, unescapeToRope(m.group(1)));
        }

        private void handlePost(int round, FullHttpRequest req) {
            String ct = req.headers().get(CONTENT_TYPE);
            var r = new SegmentRope(req.content().nioBuffer());
            if (indexOfIgnoreCaseAscii(ct, APPLICATION_X_WWW_FORM_URLENCODED, 0) == 0) {
                int begin = 0, len = r.len;
                while (begin < len && !r.hasAnyCase(begin, QUERY_EQ))
                    begin = r.skipUntil(begin, len, '&')+1;
                begin += QUERY_EQ.length;
                if (begin >= len)
                    endRound(0, BAD_REQUEST, "No query in form",  null);
                else
                    handleQuery(round, unescape(r, begin, r.skipUntil(begin, r.len, '&')));
            } else if (indexOfIgnoreCaseAscii(ct, APPLICATION_SPARQL_QUERY, 0) == 0) {
                handleQuery(round, r);
            } else {
                endRound(0, UNSUPPORTED_MEDIA_TYPE, "Expected Content-Type to be application/x-www-form-urlencoded or application/sparql-query, got "+ct, null);
            }
        }

        private void handleQuery(int round, SegmentRope sparqlRope) {
            if (parseQuery(sparqlRope, 0) && dispatchQuery(null, BindType.JOIN)) {
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
                    if (!atRound(round)) return;
                    serializer.serialize(b, sink);
                }
                serializer.serializeTrailer(sink);
                ByteBuf bb = sink.take();
                var res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_0, OK, bb);
                res.headers().set(CONTENT_TYPE, serializer.contentType())
                             .set(CONTENT_LENGTH, bb.readableBytes());
                if (endRound(round, OK, null, null))
                    ctx.writeAndFlush(res);
                else
                    res.release();
            } catch (Throwable t) {
                endRound(round, INTERNAL_SERVER_ERROR, null, t);
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
            endRound(0, NOT_ACCEPTABLE, noSerializerError, null);
            return false;
        }
    }

    private final class WsSparqlHandler extends QueryHandler<WebSocketFrame> implements WsFrameSender<ByteBufSink> {
        private static final byte[] QUERY = "!query".getBytes(UTF_8);
        private static final byte[] JOIN = "!join".getBytes(UTF_8);
        private static final byte[] LEFT_JOIN = "!left-join".getBytes(UTF_8);
        private static final byte[] EXISTS = "!exists".getBytes(UTF_8);
        private static final byte[] MINUS = "!minus".getBytes(UTF_8);
        private static final byte[] NOT_EXISTS = "!not-exists".getBytes(UTF_8);
        private static final byte[] CANCEL = "!cancel".getBytes(UTF_8);
        private static final byte[] ERROR = "!error ".getBytes(UTF_8);
        private static final byte[] CANCELLED = "!cancelled ".getBytes(UTF_8);

        private final SegmentRope wrapperRope = new SegmentRope();
        private @MonotonicNonNull ByteBufSink fsSink;
        private final int maxBindings = wsServerBindings();

        private @Nullable WsServerParserBIt<CompressedBatch> bindingsParser;
        private byte @MonotonicNonNull [] fullBindReq, halfBindReq;
        private int requestBindingsAt;
        private int waitingVarsRound;
        private BindType bType = BindType.JOIN;

        public WsSparqlHandler() {
            serializer = new WsSerializer();
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

        @Override protected void sendChunk(ByteBuf bb) {
            ctx.writeAndFlush(new TextWebSocketFrame(bb));
        }

        /* --- --- --- SimpleChannelInboundHandler methods and request handling --- --- --- */

        @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
            fsSink = new ByteBufSink(ctx.alloc());
            super.channelActive(ctx);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            fsSink.release();
            super.channelInactive(ctx);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame msg) {
            wrapperRope.wrapBuffer(msg.content().nioBuffer());
            byte f = wrapperRope.len < 2 ? 0 : wrapperRope.get(1);
            try {
                if (f == 'c' && wrapperRope.has(0, CANCEL)) {
                    endRound(0, INTERNAL_SERVER_ERROR, null, wsCancelledEx);
                } else if (waitingVarsRound > 0) {
                    readVarsFrame();
                } else if (bindingsParser != null) {
                    readBindings();
                } else {
                    handleQueryCommand(ctx, f);
                }
            } finally {
                wrapperRope.wrapEmptyBuffer();
            }
        }

        private void handleQueryCommand(ChannelHandlerContext ctx, byte f) {
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
            if (ex == null || !wrapperRope.has(0, ex)) {
                fail(new ByteRope().append("Unexpected frame: ").appendEscapingLF(wrapperRope));
            } else if (parseQuery(wrapperRope, ex.length)) {
                if (waitingVarsRound > 0) {
                    requestBindingsAt = maxBindings>>1;
                    if (fullBindReq == null) {
                        fullBindReq = ("!bind-request "+maxBindings+"\n").getBytes(UTF_8);
                        halfBindReq = ("!bind-request "+(maxBindings>>1)+"\n").getBytes(UTF_8);
                    }
                    ctx.writeAndFlush(new TextWebSocketFrame(
                            ctx.alloc().buffer().writeBytes(fullBindReq)));
                } else {
                    if (dispatchQuery(null, BindType.JOIN))
                        Thread.startVirtualThread(() -> drainerThread(round));
                }
            }
        }

        private void readBindings() { //noinspection DataFlowIssue
            bindingsParser.feedShared(wrapperRope);
            if (bindingsParser.rowsEmitted() >= requestBindingsAt) {
                requestBindingsAt += maxBindings>>1;
                ctx.writeAndFlush(new TextWebSocketFrame(ctx.alloc().buffer(32)
                        .writeBytes(halfBindReq)));
            }
        }

        private void readVarsFrame() {
            if (query == null && fail("null query")) return;
            int len = wrapperRope.len, eol = wrapperRope.skipUntil(0, len, '\n');
            if (eol == len && fail("No LF (\\n) in vars after !bind frame"))
                return;
            Vars vars = new Vars.Mutable(query.publicVars().size());
            for (int i = 0, j; i < eol; i = j+1) {
                byte c = wrapperRope.get(i);
                if (c != '?' && c != '$' && fail("Missing ?/$ in var name"))
                    return;
                j = wrapperRope.skipUntil(i, len, '\t',  '\n');
                vars.add(new ByteRope(j-i-1).append(wrapperRope, i+1, j));
            }
            bindingsParser = new WsServerParserBIt<>(this, COMPRESSED, vars,
                                                     wsServerBindings());
            // do not block if client starts flooding
            bindingsParser.maxReadyItems(Integer.MAX_VALUE);
            int round = waitingVarsRound;
            waitingVarsRound = 0;
            dispatchQuery(bindingsParser, bType);
            Thread.startVirtualThread(() -> drainerThread(round));
            readBindings();
        }

        /* --- --- --- WsFrameSender --- --- --- */

        @Override public void        sendFrame(ByteBufSink content) { sendChunk(content.take()); }
        @Override public ByteBufSink createSink()                   { return fsSink.touch(); }
        @Override public void        releaseSink(ByteBufSink sink)  { sink.release(); }
    }
}
