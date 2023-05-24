package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.netty.util.ByteBufSink;
import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRopeUtils;
import com.github.alexishuf.fastersparql.client.netty.util.NettySPSCBIt;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClientHandler;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.SparqlResultFormat;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.WsClientParserBIt;
import com.github.alexishuf.fastersparql.sparql.results.WsFrameSender;
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

import static com.github.alexishuf.fastersparql.model.BindType.MINUS;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NettyWsSparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(NettyWsSparqlClient.class);
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

    @Override
    public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql) {
        if (sparql.isGraph())
            throw new FSInvalidArgument("query() method only takes SELECT/ASK queries");
        try {
            return new WsBIt<>(batchType, sparql);
        } catch (Throwable t) { throw FSException.wrap(endpoint, t); }
    }

    @Override
    public <B extends Batch<B>>
    BIt<B> query(BatchType<B> batchType, SparqlQuery sp, @Nullable BIt<B> bindings,
                 @Nullable BindType bindType, @Nullable JoinMetrics metrics) {
        if (bindings == null || bindings instanceof EmptyBIt)
            return query(batchType, sp);
        if (bindType == null)
            throw new FSInvalidArgument("bindType is null with non-null bindings");
        if (sp.isGraph())
            throw new FSInvalidArgument("query() method only takes SELECT/ASK queries");
        try {
            if (bindType == MINUS && !bindings.vars().intersects(sp.allVars()))
                return bindings;
            else if (!bindType.isJoin())
                sp = sp.toAsk();
            Vars exVars = bindType.resultVars(bindings.vars(), sp.publicVars());
            return new WsBIt<>(batchType, sp, exVars, bindType, bindings, metrics);
        } catch (Throwable t) { throw FSException.wrap(endpoint, t); }
    }

    @Override public void close() { this.netty.close(); }

    /* --- --- --- helper methods --- --- --- */

    private static final byte[] QUERY_VERB = "!query ".getBytes(UTF_8);
    private static final byte[][] BIND_VERB = {
            "!join "      .getBytes(UTF_8),
            "!left-join " .getBytes(UTF_8),
            "!exists "    .getBytes(UTF_8),
            "!not-exists ".getBytes(UTF_8),
            "!minus "     .getBytes(UTF_8),
    };
    private static Rope createRequest(byte[] verb, Rope sparql) {
        int lfCount = sparql.get(sparql.len()-1) != '\n' ? 1 : 0;
        ByteRope r = new ByteRope(verb.length + sparql.len() + lfCount);
        r.append(verb).append(sparql);
        if (lfCount > 0) r.append('\n');
        return r;
    }

    /* --- --- --- BIt implementations --- --- --- */

    private class WsBIt<B extends Batch<B>> extends NettySPSCBIt<B> implements NettyWsClientHandler,
            WsFrameSender<ByteBufSink> {
        private final Rope requestMessage;
        private final WsClientParserBIt<B> parser;
        private boolean gotFrames = false;
        private final SegmentRope bufferRope = new SegmentRope();
        protected @MonotonicNonNull ChannelRecycler recycler;
        protected final ByteBufSink bbSink = new ByteBufSink(UnpooledByteBufAllocator.DEFAULT);

        public WsBIt(BatchType<B> batchType, SparqlQuery query) {
            super(batchType, query.publicVars(), FSProperties.queueMaxRows());
            this.requestMessage = createRequest(QUERY_VERB, query.sparql());
            this.parser = new WsClientParserBIt<>(this, this);
            request();
        }

        public WsBIt(BatchType<B> batchType, SparqlQuery query, Vars outVars, BindType bindType, BIt<B> bindings,
                     @Nullable JoinMetrics metrics) {
            super(batchType, outVars, FSProperties.queueMaxRows());
            this.requestMessage = createRequest(BIND_VERB[bindType.ordinal()], query.sparql());
            var usefulBindingVars = bindings.vars().intersection(query.allVars());
            this.parser = new WsClientParserBIt<>(this, this, bindings, usefulBindingVars, metrics);
            request();
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            super.cleanup(cause);
            bbSink.release();
            bbSink.alloc(UnpooledByteBufAllocator.DEFAULT);
        }

        /* --- --- --- WsFrameSender methods --- --- --- */

        @Override public void sendFrame(ByteBufSink content) {
            if (terminated) {
                //noinspection RedundantCast
                log.debug("{}: ignoring sendFrame({}) after complete({})", this, content, (Object)error);
                content.release();
                return;
            }
            final Channel ch = this.channel;
            if (ch == null)
                throw new IllegalStateException("sendFrame() before attach()");
            ch.writeAndFlush(new TextWebSocketFrame(content.take()));
        }

        @Override public ByteBufSink createSink() {
            return bbSink.touch();
        }

        @Override public void releaseSink(ByteBufSink sink) { sink.release(); }

        /* --- --- --- NettySPSCBIt methods --- --- --- */

        @Override public    SparqlClient      client() { return NettyWsSparqlClient.this; }
        @Override protected void             request() { netty.open(this); }
        @Override protected void afterNormalComplete() { recycler.recycle(channel); }

        /* --- --- --- WsClientHandler methods --- --- --- */

        @Override public void attach(ChannelHandlerContext ctx, ChannelRecycler recycler) {
            assert channel == null : "previous attach()";
            this.recycler = recycler;
            if (isCompleted()) {
                this.recycler.recycle(ctx.channel());
                return;
            }
            this.channel = ctx.channel();
            bbSink.alloc(this.channel.alloc());
            ctx.writeAndFlush(new TextWebSocketFrame(NettyRopeUtils.wrap(requestMessage, UTF_8)));
        }

        @Override public void detach(Throwable cause) {
            if (!isCompleted()) { // flush parser, which may call end() or onError(String)
                parser.complete(null);
                if (!isCompleted()) {
                    if (cause == null) {
                        cause = new FSServerException("Connection closed before "
                                + (gotFrames ? "!end but after " : "") + "starting a response"
                        ).shouldRetry(!gotFrames);
                    }
                    complete(cause);
                }
            }
        }

        @Override public void frame(WebSocketFrame frame) {
            gotFrames = true;
            if (frame instanceof TextWebSocketFrame t) {
                bufferRope.wrapBuffer(t.content().nioBuffer());
                parser.feedShared(bufferRope);
            } else if (!terminated && !(frame instanceof CloseWebSocketFrame)) {
                var suffix = frame == null ? "null frame" : frame.getClass().getSimpleName();
                complete(new FSServerException("Unexpected "+suffix));
            }
        }
    }
}
