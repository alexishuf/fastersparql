package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
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
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSIllegalStateException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.sparql.results.ResultsSender;
import com.github.alexishuf.fastersparql.sparql.results.WsClientParser;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
import com.github.alexishuf.fastersparql.sparql.results.serializer.WsSerializer;
import com.github.alexishuf.fastersparql.util.StreamNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink.adjustSizeHint;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NettyWsSparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(NettyWsSparqlClient.class);

    private final NettyWsClient netty;
    private int bindingsSizeHint = WsSerializer.DEF_BUFFER_HINT;

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
        ByteRope msg = createRequest(QUERY_VERB, sparql.sparql(), null);
        return new WsBIt<>(msg, bt, sparql.publicVars(), null);
    }

    @Override
    protected <B extends Batch<B>> Emitter<B> doEmit(BatchType<B> bt, SparqlQuery sparql,
                                                     Vars rebindHint) {
        return new WsEmitter<>(bt, sparql.publicVars(), sparql, null);
    }

    @Override public <B extends Batch<B>> BIt<B> doQuery(ItBindQuery<B> bq) {
        ByteRope msg = createRequest(BIND_VERB[bq.type.ordinal()], bq.query.sparql(), null);
        return new WsBIt<>(msg, bq.batchType(), bq.resultVars(), bq);
    }

    @Override protected <B extends Batch<B>> Emitter<B> doEmit(EmitBindQuery<B> query,
                                                               Vars rebindHint) {
        BatchType<B> bt = query.bindings.batchType();
        return new WsEmitter<>(bt, query.resultVars(), query.query, query);
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

    private static ByteRope createRequest(byte[] verb, Rope sparql, @Nullable ByteRope offer) {
        int addLF = sparql.get(sparql.len()-1) != '\n' ? 1 : 0;
        int required = verb.length + sparql.len() + addLF;
        if (offer == null) offer = new ByteRope(required);
        else               offer.clear().ensureFreeCapacity(required);

        offer.append(verb).append(sparql);
        if (addLF != 0) offer.append('\n');
        return offer;
    }

    private void adjustBindingsSizeHint(int observed) {
        bindingsSizeHint = adjustSizeHint(bindingsSizeHint, observed);
    }

    /* --- --- --- BIt/Emitter implementations --- --- --- */

    private final class WsBIt<B extends Batch<B>> extends NettySPSCBIt<B> {
        private final WsHandler<B> handler;

        public WsBIt(ByteRope requestMsg, BatchType<B> batchType, Vars vars,
                     @Nullable BindQuery<B> bindQuery) {
            super(batchType, vars, FSProperties.queueMaxRows(), NettyWsSparqlClient.this);
            this.handler = new WsHandler<>(requestMsg, this, bindQuery);
            acquireRef();
            request();
        }

        @Override public String journalName() {
            return "C.WB:" + (channel == null ? "null" : channel.id().asShortText());
        }

        @Override protected void cleanup(@Nullable Throwable e) { releaseRef(); }
        @Override protected void request()                      { netty.open(handler); }
    }

    private final class WsEmitter<B extends Batch<B>> extends NettyCallbackEmitter<B> {
        private final SparqlQuery query;
        private final @Nullable EmitBindQuery<B> bindQuery;
        private @Nullable ByteRope request;
        private @Nullable WsHandler<B> handler;
        private @Nullable BatchBinding binding;

        public WsEmitter(BatchType<B> batchType, Vars outVars, SparqlQuery query, @Nullable EmitBindQuery<B> bindQuery) {
           super(batchType, outVars, NettyWsSparqlClient.this);
           this.query = query;
           this.bindQuery = bindQuery;
           acquireRef();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return bindQuery == null ? Stream.empty() : Stream.of(bindQuery.bindings);
        }

        @Override public String journalName() {
            return String.format("C.EQ:%s@%x",
                    channel == null ? "null" : channel.id().asShortText(),
                    System.identityHashCode(this));
        }

        private WsHandler<B> makeHandler() {
            var query    = binding   == null ? this.query : this.query.bound(binding);
            var verb     = bindQuery == null ? QUERY_VERB : BIND_VERB[bindQuery.type.ordinal()];
            this.request = createRequest(verb, query.sparql(), this.request);
            return new WsHandler<>(request, this, bindQuery);
        }

        @Override protected void doRelease() {
            releaseRef();
        }
        @Override protected void request()   {
            if (handler == null)
                handler = makeHandler();
            netty.open(handler);
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            int st = resetForRebind(0, LOCKED_MASK);
            try {
                assert binding.batch != null && binding.row < binding.batch.rows;
                this.binding = binding;
                WsHandler<B> h = handler;
                if (h != null)
                    h.parser.feedEnd();
                handler = null;
            } finally {
                unlock(st);
            }
        }

        @Override public Vars bindableVars() { return query.allVars(); }
    }

    private final class WsHandler<B extends Batch<B>>
            implements NettyWsClientHandler, WsFrameSender<ByteBufSink, ByteBuf>, ChannelBound {
        private static final byte[] CANCEL_MSG = "!cancel\n".getBytes(UTF_8);
        private final ByteRope requestMsg;
        private boolean gotFrames;
        private @Nullable ByteBufRopeView bbRopeView;
        private final WsClientParser<B> parser;
        private @MonotonicNonNull ChannelHandlerContext ctx;
        private @MonotonicNonNull ChannelRecycler recycler;
        private final CompletableBatchQueue<B> destination;

        public WsHandler(ByteRope requestMsg, CompletableBatchQueue<B> destination,
                         @Nullable BindQuery<B> bq) {
            this.requestMsg = requestMsg;
            this.destination = destination;
            if (bq == null) {
                parser = new WsClientParser<>(destination);
            } else {
                var useful = bq.bindingsVars().intersection(bq.query.publicVars());
                parser = new WsClientParser<>(destination, bq, useful);
            }
        }

        @Override public @Nullable Channel channel() {
            return this.ctx == null ? null : this.ctx.channel();
        }

        @Override public String journalName() {
            return "C.WH:" + (ctx == null ? "null" : ctx.channel().id().asShortText());
        }
        /* --- --- --- NettyWsClientHandler methods --- --- --- */

        @Override public void attach(ChannelHandlerContext ctx, ChannelRecycler recycler) {
            assert this.ctx == null : "previous attach()";
            this.recycler   = recycler;
            this.ctx        = ctx;
            this.bbRopeView = ByteBufRopeView.create();
            this.parser.setFrameSender(this);
            if (destination.isTerminated()) { // cancel()ed before WebSocket established
                recycler.recycle(ctx.channel());
            } else {
                var bb = Unpooled.wrappedBuffer(requestMsg.backingArray(),
                                                requestMsg.backingArrayOffset(), requestMsg.len);
                ctx.writeAndFlush(new TextWebSocketFrame(bb));
            }
        }

        @Override public void detach(@Nullable Throwable error) { complete(error); }

        @Override public void frame(WebSocketFrame frame) {
            if (frame instanceof TextWebSocketFrame f) {
                if (bbRopeView == null)
                    return; // ignore frame after complete
                gotFrames = true;
                try {
                    parser.feedShared(bbRopeView.wrapAsSingle(f.content()));
                } catch (TerminatedException|CancelledException e) {
                    sendCancel();
                }
            } else if (frame instanceof CloseWebSocketFrame) {
                complete(null);
            } else {
                complete(new FSServerException("Unexpected frame type: "));
            }
        }

        /* --- --- --- private helpers --- --- --- */

        private void completeWithError(@Nullable Throwable error) {
            if (destination.isTerminated()) {
                String now = error == null ? "complete(null)"
                           : error.getClass().getSimpleName();
                String previous = destination.error() != null
                        ? ("failed with "+destination.error())
                        : (destination.isCancelled() ? "cancelled" : "completed");
                log.warn("Ignoring {} since {} was already {}",
                         now, destination, previous, error);
                return;
            }
            if (error instanceof FSServerException se && gotFrames) {
                se.shouldRetry(false);
            } else if (error == null) {
                if (!gotFrames)
                    error = new InvalidSparqlResultsException("Empty response").shouldRetry(true);
                else
                    error = new FSIllegalStateException("coldComplete unsatisfied preconditions");
            }
            parser.feedError(FSException.wrap(endpoint, error));
        }

        private void complete(@Nullable Throwable error) {
            if (gotFrames && error == null) parser.feedEnd();
            else                            completeWithError(error); // (should be) cold
            if (bbRopeView != null) {
                bbRopeView.recycle();
                bbRopeView = null;
                recycler.recycle(ctx.channel());
            }
        }

        private void sendCancel() {
            if (bbRopeView == null || ctx == null || !ctx.channel().isActive())
                return; // do not send frame after complete() or before attach()
            ctx.writeAndFlush(new TextWebSocketFrame(Unpooled.wrappedBuffer(CANCEL_MSG)));
        }

        /* --- --- --- WsFrameSender methods --- --- --- */

        @Override public void sendFrame(ByteBuf content) {
            if (ctx == null) {
                throw new IllegalStateException("sendFrame() before attach()");
            } else if (bbRopeView == null) {
                log.debug("{}.sendFrame() after complete(), dropping {}", this, content);
                content.release();
            } else {
                ctx.writeAndFlush(new TextWebSocketFrame(content));
            }
        }

        @Override public ByteBufSink createSink() {
            return new ByteBufSink(ctx == null ? UnpooledByteBufAllocator.DEFAULT : ctx.alloc());
        }

        /* --- --- --- ResultsSender --- --- --- */

        @Override public ResultsSender<ByteBufSink, ByteBuf> createSender() {
            if (ctx == null)
                throw new IllegalStateException("createSender() before attach()");
            return new BindingsSender(ctx);
        }

        private final class BindingsSender extends NettyResultsSender<TextWebSocketFrame> {
            public BindingsSender(ChannelHandlerContext ctx) {
                super(WsSerializer.create(bindingsSizeHint), ctx);
                sink.sizeHint(bindingsSizeHint);
            }

            @Override protected void onRelease() {
                super.onRelease();
                ((WsSerializer)serializer).recycle();
            }

            @Override public void sendTrailer() {
                adjustBindingsSizeHint(sink.sizeHint());
                super.sendTrailer();
            }

            @Override public void sendError(Throwable t) {
                var escaped = t.toString().replace("\n", "\\n");
                execute(Unpooled.copiedBuffer("!error "+escaped+"\n", UTF_8));
            }

            @Override protected void               onError(Throwable t) { complete(t); }
            @Override public    void               sendCancel()         { WsHandler.this.sendCancel(); }
            @Override protected TextWebSocketFrame wrap(ByteBuf bb)     { return new TextWebSocketFrame(bb); }
        }
    }
}
