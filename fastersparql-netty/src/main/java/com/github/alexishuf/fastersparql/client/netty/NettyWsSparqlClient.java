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
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
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
            this.handler = new WsHandler<>(requestMsg, this, true, bindQuery);
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
        private final WsHandler<B> handler;
        private ByteRope request;
        private final @Nullable EmitBindQuery<B> bindQuery;
        private @Nullable BatchBinding binding;

        public WsEmitter(BatchType<B> batchType, Vars outVars, SparqlQuery query,
                         @Nullable EmitBindQuery<B> bindQuery) {
           super(batchType, outVars, NettyWsSparqlClient.this);
           this.query = query;
           this.bindQuery = bindQuery;
           this.handler = new WsHandler<>(makeRequest(), this, false, bindQuery);
           acquireRef();
        }

        @Override public void setChannel(Channel channel) { super.setChannel(channel); }

        @Override protected void appendToSimpleLabel(StringBuilder out) {
            out.append(" ch=").append(channel);
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            var rcv = handler.parser.bindingsReceiver();
            return Stream.ofNullable(rcv);
        }

        @Override public String journalName() {
            return String.format("C.WE:%s@%x",
                    channel == null ? "null" : channel.id().asShortText(),
                    System.identityHashCode(this));
        }

        private ByteRope makeRequest() {
            var query    = binding   == null ? this.query : this.query.bound(binding);
            var verb     = bindQuery == null ? QUERY_VERB : BIND_VERB[bindQuery.type.ordinal()];
            var req      = createRequest(verb, query.sparql(), this.request);
            this.request = req;
            return req;
        }

        @Override protected void doRelease() {
            try {
                releaseRef();
                handler.reset(null);
            } finally { super.doRelease(); }
        }
        @Override protected void   request() { netty.open(handler); }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            int st = resetForRebind(0, LOCKED_MASK);
            try {
                assert binding.batch != null && binding.row < binding.batch.rows;
                this.binding = binding;
                if (bindQuery != null)
                    bindQuery.bindings.rebind(binding);
                handler.reset(makeRequest());
            } finally {
                unlock(st);
            }
        }

        @Override public Vars bindableVars() { return query.allVars(); }
    }

    private final class WsHandler<B extends Batch<B>>
            implements NettyWsClientHandler, WsFrameSender<ByteBufSink, ByteBuf>, ChannelBound {
        private static final byte[] CANCEL_MSG = "!cancel\n".getBytes(UTF_8);
        private ByteRope requestMsg;
        private boolean gotFrames;
        private final boolean selfRecycle;
        private @Nullable ByteBufRopeView bbRopeView;
        private final WsClientParser<B> parser;
        private @Nullable ChannelHandlerContext ctx;
        private @MonotonicNonNull ChannelRecycler recycler;
        private final CompletableBatchQueue<B> destination;
        private @MonotonicNonNull Channel lastCh;

        public WsHandler(ByteRope requestMsg, CompletableBatchQueue<B> destination,
                         boolean selfRecycle, @Nullable BindQuery<B> bq) {
            this.requestMsg = requestMsg;
            this.destination = destination;
            this.selfRecycle = selfRecycle;
            if (bq == null) {
                parser = new WsClientParser<>(destination);
            } else {
                var useful = bq.bindingsVars().intersection(bq.query.publicVars());
                parser = new WsClientParser<>(destination, bq, useful);
            }
        }

        NettyWsSparqlClient sparqlClient() { return NettyWsSparqlClient.this; }

        void reset(@Nullable ByteRope requestMsg) {
            var ctx = this.ctx;
            if (ctx != null) {
                var ch = ctx.channel();
                var exec = ctx.executor();
                if (exec.inEventLoop()) doReset(requestMsg, ch);
                else                    exec.execute(() -> doReset(requestMsg, ch));
            } else if (requestMsg != null) {
                this.requestMsg = requestMsg;
            }
        }

        private void doReset(@Nullable ByteRope requestMsg, @Nullable Channel ch) {
            assert ctx == null || ctx.executor().inEventLoop() : "not in event loop";
            parser.reset();
            if (requestMsg != null) {
                this.requestMsg = requestMsg;
                gotFrames       = false;
            }  else {
                var bbRopeView = this.bbRopeView;
                if (bbRopeView != null) {
                    bbRopeView.recycle();
                    this.bbRopeView = null;
                }
            }
            if (ctx != null && ctx.channel() == ch) {
                this.ctx = null;
                recycler.recycle(ch);
            }


        }

        /* --- --- --- ChannelBound methods --- --- --- */

        @Override public @Nullable Channel channel() { return lastCh; }

        @Override public String journalName() {
            var id = lastCh == null ? "null" : lastCh.id().asShortText();
            return "C.WH:"+id+'@'+Integer.toHexString(System.identityHashCode(this));
        }

        /* --- --- --- NettyWsClientHandler methods --- --- --- */

        @Override public void attach(ChannelHandlerContext ctx, ChannelRecycler recycler) {
            assert this.ctx == null : "previous attach()";
            Channel ch = ctx.channel();
            this.recycler = recycler;
            this.ctx      = ctx;
            this.lastCh   = ch;
            if (destination instanceof WsEmitter<B> e)
                e.setChannel(ch);
            if (bbRopeView == null)
                bbRopeView = ByteBufRopeView.create();
            parser.setFrameSender(this);
            if (destination.isTerminated()) { // cancel()ed before WebSocket established
                recycler.recycle(ch);
            } else {
                var bb = Unpooled.wrappedBuffer(requestMsg.backingArray(),
                                                requestMsg.backingArrayOffset(), requestMsg.len);
                ctx.writeAndFlush(new TextWebSocketFrame(bb));
            }
        }

        @Override public void detach(@Nullable Throwable error) {
            if (ctx == null)
                return; // indirectly called from recycle()
            complete(error);
        }

        @Override public void frame(WebSocketFrame frame) {
            if (frame instanceof TextWebSocketFrame f) {
                if (bbRopeView == null)
                    return; // ignore frame after complete
                gotFrames = true;
                try {
                    parser.feedShared(bbRopeView.wrapAsSingle(f.content()));
                    if (selfRecycle && destination.isTerminated())
                        reset(null);
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
            if (!destination.isTerminated()) {
                if (gotFrames && error == null) parser.feedEnd();
                else                            completeWithError(error); // (should be) cold
            }
            if (selfRecycle)
                reset(null);
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
            return new BindingsSender<>(this, ctx);
        }

        private static final class BindingsSender<B extends Batch<B>> extends NettyResultsSender<TextWebSocketFrame> {
            private final WsHandler<B> wsHandler;
            boolean recycledSerializer;

            private static final class RecycleAction extends Action {
                public static final RecycleAction INSTANCE = new RecycleAction();
                public RecycleAction() {super("RECYCLE");}

                @Override public void run(NettyResultsSender<?> sender) {
                    var bs = (BindingsSender<?>) sender;
                    if (!bs.recycledSerializer) {
                        bs.recycledSerializer = true;
                        ((WsSerializer)bs.serializer).recycle();
                    }
                }
            }

            public BindingsSender(WsHandler<B> wsHandler, ChannelHandlerContext ctx) {
                super(WsSerializer.create(wsHandler.sparqlClient().bindingsSizeHint), ctx);
                this.wsHandler = wsHandler;
                sink.sizeHint(wsHandler.sparqlClient().bindingsSizeHint);
            }

            @Override public void close() {
                super.close();
                if (!recycledSerializer)
                    execute(RecycleAction.INSTANCE);
            }

            @Override public void sendInit(Vars vars, Vars subset, boolean isAsk) {
                if (recycledSerializer) throw new IllegalStateException("recycled");
                super.sendInit(vars, subset, isAsk);
            }

            @Override public void sendSerializedAll(Batch<?> batch) {
                if (recycledSerializer) throw new IllegalStateException("recycled");
                super.sendSerializedAll(batch);
            }

            @Override public void sendSerialized(Batch<?> batch, int from, int nRows) {
                if (recycledSerializer) throw new IllegalStateException("recycled");
                super.sendSerialized(batch, from, nRows);
            }

            @Override public void sendTrailer() {
                wsHandler.sparqlClient().adjustBindingsSizeHint(sink.sizeHint());
                super.sendTrailer();
            }

            @Override public void sendError(Throwable t) {
                journal("sendError", t, "sender=", this);
                var escaped = t.toString().replace("\n", "\\n");
                execute(Unpooled.copiedBuffer("!error "+escaped+"\n", UTF_8),
                        Action.RELEASE_SINK);
            }

            private static final class CancelAction extends Action {
                private final WsHandler<?> handler;
                public CancelAction(WsHandler<?> handler) {
                    super("CANCEL");
                    this.handler = handler;
                }
                @Override public void run(NettyResultsSender<?> sender) { handler.sendCancel(); }
            }

            @Override public void  sendCancel() {
                journal("sendCancel, sender=", this);
                execute(new CancelAction(wsHandler), Action.RELEASE_SINK);
            }

            @Override protected void onError(Throwable t) {
                journal("onError", t, "sender=", this);
                wsHandler.complete(t);
            }

            @Override protected TextWebSocketFrame wrap(ByteBuf bb) {
                return new TextWebSocketFrame(bb);
            }
        }
    }
}
