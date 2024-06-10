package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient.ConnectionHandler;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpHandler;
import com.github.alexishuf.fastersparql.client.netty.util.ByteBufRopeView;
import com.github.alexishuf.fastersparql.client.netty.util.ChannelBound;
import com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRopeUtils;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.async.CallbackEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.MutableRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRopeView;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeRegistry;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.client.netty.util.NettyRopeUtils.asByteBuf;
import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.fromMediaType;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class NettySparqlClient extends AbstractSparqlClient {
    private static final Set<SparqlMethod> SUPPORTED_METHODS
            = Set.of(SparqlMethod.GET, SparqlMethod.FORM, SparqlMethod.POST);
    private final NettyHttpClient netty;

    public NettySparqlClient(SparqlEndpoint ep) {
        super(withSupported(ep, SUPPORTED_METHODS));
        try {
            Supplier<NettyHandler> handlerFac = NettyHandler::new;
            this.netty = new NettyClientBuilder().buildHTTP(ep.uri(), handlerFac);
        } catch (Throwable t) {
            throw FSException.wrap(ep, t);
        }
    }

    @Override public SparqlClient.Guard retain() { return new RefGuard(); }

    @Override protected void doClose() { netty.close(); }

    @Override protected <B extends Batch<B>> BIt<B> doQuery(BatchType<B> bt, SparqlQuery sp) {
        return new QueryBIt<>(bt, sp) ;
    }

    @Override
    protected <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    doEmit(BatchType<B> bt, SparqlQuery sparql, Vars rebindHint) {
        return new QueryEmitter<>(bt, sparql);
    }

    /* --- --- --- helper methods  --- --- ---  */

    private static HttpMethod method2netty(SparqlMethod method) {
        return switch (method) {
            case GET -> HttpMethod.GET;
            case POST, FORM -> HttpMethod.POST;
            default -> throw new IllegalArgumentException("Unexpected " + method);
        };
    }

    private FullHttpRequest createRequest(SparqlQuery qry) {
        var cfg = endpoint.configuration();
        SegmentRope sparql = qry.sparql();
        var method = method(cfg, sparql.len());
        ByteBuf body = switch (method) {
            case POST -> asByteBuf(sparql);
            case FORM -> {
                try (var rope = formString(sparql, cfg.params())) {
                    yield NettyRopeUtils.asByteBuf(rope);
                }
            }
            case GET  -> null;
            default   -> throw new FSInvalidArgument(method+" not supported by "+this);
        };
        String pathAndParams = firstLine(endpoint, cfg, body == null ? sparql : null);
        String accept        = qry.isGraph() ? rdfAcceptString(cfg.rdfAccepts())
                                           : resultsAcceptString(cfg.resultsAccepts());
        if (body == null)
            return NettyHttpClient.makeGet(pathAndParams, accept);
        return NettyHttpClient.makeRequest(method2netty(method), pathAndParams, accept,
                                           method.contentType(), body);
    }

    /* --- --- --- inner classes --- --- ---  */

    /** Asynchronous {@code BIt} that sends an HTTP request on first access
     *  and is fed with results from the server.
     *
     *  <p>This object is distinct from {@link NettyHandler} since it is unique to a query,
     *  while {@link NettyHandler}'s lifecycle is attached to the Netty channel which may be
     *  reused for multiple SPARQL queries.</p>
     */
    private class QueryBIt<B extends Batch<B>> extends SPSCBIt<B> implements ClientStreamNode<B> {
        private final FullHttpRequest request;
        private @Nullable NettyHandler handler;
        private int retries;
        private boolean backPressured;
        private @Nullable Channel lastCh;
        private @Nullable String info;
        private int cookie;

        public QueryBIt(BatchType<B> batchType, SparqlQuery query) {
            super(batchType, query.publicVars());
            this.request = createRequest(query);
            acquireRef();
            netty.connect(this);
        }

        /* --- --- --- ChannelBound --- --- -- */

        @Override public @Nullable Channel channelOrLast() { return lastCh; }

        @Override public void setChannel(Channel ch) {
            if (ch != null && ch != lastCh) throw new UnsupportedOperationException();
        }

        @Override public String journalName() {
            return "C.QB:"+(lastCh==null ? "null" : lastCh.id().asShortText())+'@'+id();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return StreamNodeRegistry.stream(info);
        }

        @Override protected StringBuilder minimalLabel() {
            return new StringBuilder().append("C.QB:")
                    .append(lastCh == null ? "null" : lastCh.id().asShortText())
                    .append('@').append(id());
        }

        @Override protected void appendToSimpleLabel(StringBuilder sb) {
            super.appendToSimpleLabel(sb);
            if (info != null)
                sb.append(" info=").append(info);
        }

        @Override protected void appendStateToLabel(StringBuilder sb) {
            super.appendStateToLabel(sb);
            NettyHandler h = handler;
            if (h == null)
                sb.append(" handler=null");
            else
                sb.append(" handler.st=").append(h.currentState());
        }

        /* --- --- --- ClientStreamNode --- --- -- */

        @Override public int      cookie()                   { return cookie; }
        @Override public void setInfo(@Nullable String info) { this.info = info; }

        @Override public boolean retry() {
            lock();
            try {
                boolean terminated = isTerminated();
                if (!terminated && retries < FSNettyProperties.maxRetries()) {
                    ++retries;
                    journal("retry ", retries, "on", this);
                    netty.connect(this);
                    return true;
                }
                journal("will not retry, term=", terminated?1:0, "retries=", retries,
                        "on", this);
            } finally { unlock(); }
            return false;
        }

        /* --- --- --- ConnectionHandler --- --- -- */

        @Override public HttpRequest     httpRequest()              { return request.retain(); }
        @Override public NettyHttpClient  httpClient()              { return netty; }
        @Override public void      onConnectionError(Throwable t)   { complete(t); }

        @Override public void onStarted(NettyHandler h, int c) {
            lock();
            try { this.cookie = c; } finally { unlock(); }
        }

        @Override public void onConnected(Channel ch, NettyHandler handler) {
            this.lastCh = ch;
            boolean abort;
            lock();
            try {
                abort = isTerminated();
                handler.attach(this);
                this.handler = handler;
            } finally { unlock(); }
            if (abort)
                throw NettyHttpClient.ABORT_REQUEST;
        }

        /* --- --- --- SPSCBIt --- --- -- */

        @Override protected void cleanup(@Nullable Throwable cause) {
            try {
                super.cleanup(cause);
            } finally { releaseRef(); }
        }

        @Override protected boolean mustPark(int offerRows, long queuedRows) {
            if (super.mustPark(offerRows, queuedRows)) {
                if (!backPressured) {
                    backPressured = true;
                    var handler = this.handler;
                    if (handler != null) handler.disableAutoRead();
                }
            }
            return false;
        }

        @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
            Orphan<B> b = super.nextBatch(offer);
            var handler = this.handler;
            if (backPressured && handler != null) {
                backPressured = false;
                handler.enableAutoRead();
            }
            return b;
        }

        @Override public boolean complete(@Nullable Throwable error) {
            boolean did = super.complete(error);
            if (request.refCnt() > 0)
                request.release();
            return did;
        }

        @Override public boolean cancel(boolean ack) {
            boolean did = super.cancel(ack);
            if (ack && request.refCnt() > 0)
                request.release();
            return did;
        }

        @Override public boolean tryCancel() {
            boolean did = super.tryCancel();
            if (did) {
                lock();
                try {
                    if (handler != null) handler.cancel(cookie);
                } finally { unlock(); }
            }
            return did;
        }
    }

    private final class QueryEmitter<B extends Batch<B>>
            extends CallbackEmitter<B, QueryEmitter<B>>
            implements ClientStreamNode<B>, Orphan<QueryEmitter<B>> {
        private final SparqlQuery originalQuery;
        private SparqlQuery boundQuery;
        private @Nullable FullHttpRequest boundRequest;
        private @Nullable B lb;
        private @Nullable BatchMerger<B, ?> merger;
        private @Nullable Vars mergerFreeVars;
        private @Nullable NettyHandler handler;
        private @MonotonicNonNull Channel lastCh;
        private @Nullable String info;
        private int retries;
        private int cookie;

        public QueryEmitter(BatchType<B> batchType, SparqlQuery query) {
            super(batchType, query.publicVars(), CREATED, CB_FLAGS);
            this.originalQuery = query;
            this.boundQuery    = query;
            acquireRef();
        }

        @Override public QueryEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}

        @Override protected void doRelease() {
            if (boundRequest != null) {
                boundRequest.release();
                boundRequest = null;
            }
            lb     = Batch.safeRecycle(lb,     this);
            merger = Owned.safeRecycle(merger, this);
            releaseRef();
            super.doRelease();
        }

        /* --- --- --- ChannelBound --- --- --- */

        @Override public @Nullable Channel channelOrLast() { return lastCh; }

        @Override public void setChannel(Channel ch) {
            if (ch != null && ch != lastCh) throw new UnsupportedOperationException();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return StreamNodeRegistry.stream(info);
        }

        @Override public String journalName() {
            if (journalName == null) {
                journalName = "C.QE:"+(lastCh == null ? "null" : lastCh.id().asShortText())
                            +"@"+Integer.toHexString(System.identityHashCode(this));
            }
            return journalName;
        }

        @Override protected StringBuilder minimalLabel() {
            return new StringBuilder().append("C.QE:")
                    .append(lastCh == null ? "null" : lastCh.id().asShortText())
                    .append('@').append(Integer.toHexString(System.identityHashCode(this)));
        }

        @Override protected void appendToSimpleLabel(StringBuilder out) {
            super.appendToSimpleLabel(out);
            if (info != null) out.append(" info=").append(info);
        }

        @Override protected void appendToState(StringBuilder out) {
            super.appendToState(out);
            NettyHandler h = handler;
            if (h != null)
                out.append(" handler.st=").append(handler.currentState());
            else
                out.append(" handler=null");
        }

        /* --- --- --- ClientStreamNode --- --- --- */

        @Override public int  cookie()                       { return cookie; }
        @Override public void setInfo(@Nullable String info) { this.info = info;}

        @Override public boolean retry() {
            int st = state();
            if ((st&(IS_TERM|GOT_CANCEL_REQ)) == 0 && retries < FSNettyProperties.maxRetries()) {
                ++retries;
                journal("retry", retries, "on", this);
                netty.connect(this);
                return true;
            }
            journal("will not retry, st=", st, flags, "retries=", retries,
                    "on", this);
            return false;
        }

        /* --- --- --- ConnectionHandler --- --- --- */

        @Override public HttpRequest httpRequest() {
            var request = this.boundRequest;
            if (request == null)
                this.boundRequest = request = createRequest(boundQuery);
            request.retain(); // retain for retries, Nett will release() on write()
            return request;
        }

        @Override public void onConnected(Channel ch, NettyHandler handler) {
            journalName = null;
            lastCh = ch;
            boolean abort;
            int st = lock();
            try {
                abort = (st&GOT_CANCEL_REQ) != 0;
                handler.attach(this);
                this.handler = handler;
            } finally { unlock(); }
            if (abort)
                throw NettyHttpClient.ABORT_REQUEST; // recycle ch
        }

        @Override public void     onConnectionError(Throwable cause) { complete(cause); }
        @Override public NettyHttpClient httpClient()                { return netty; }

        @Override public void onStarted(NettyHandler handler, int cookie) {
            lock();
            try { this.cookie = cookie; } finally { unlock(); }
        }

        /* --- --- --- CallbackEmitter producer actions --- --- --- */

        @Override protected void startProducer() {
            netty.connect(this);
        }

        @Override protected void pauseProducer() {
            if (this.handler != null) this.handler.disableAutoRead();
        }

        @Override protected void resumeProducer(long requested) {
            if (this.handler != null) this.handler.enableAutoRead();
        }

        @Override protected void cancelProducer() {
            lock();
            try {
                if (handler != null) handler.cancel(cookie);
            } finally { unlock(); }
        }

        @Override protected void earlyCancelProducer() {}

        @Override protected void releaseProducer() {}


        @Override public int preferredRequestChunk() {
            return 8*super.preferredRequestChunk();
        }

        @Override protected void deliver(Orphan<B> orphan) {
            if (merger != null) {
                B b = orphan.takeOwnership(this);
                orphan = merger.merge(pollDownstreamFillingBatch(), lb, 0, b);
                b.recycle(this);
            }
            super.deliver(orphan);
        }
        @Override protected void deliverByCopy(B b) {
            if (merger == null) {
                super.deliverByCopy(b);
            } else {
                Orphan<B> orphan = merger.merge(pollDownstreamFillingBatch(), lb, 0, b);
                beforeDelivery(orphan);
                deliver0(orphan);
            }
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            resetForRebind(CLEAR_ON_REBIND, LOCKED_MASK);
            try {
                boundQuery = originalQuery.bound(binding);
                var freeVars = boundQuery.publicVars();
                if (freeVars.size() == originalQuery.publicVars().size()) {
                    lb = Batch.recycle(lb, this);
                    merger = null;
                } else {
                    if (!freeVars.equals(mergerFreeVars)) {
                        Owned.safeRecycle(merger, this);
                        mergerFreeVars = freeVars;
                        merger          = bt.merger(vars, binding.vars, vars).takeOwnership(this);
                    }
                    binding.putRow(lb = bt.empty(lb, this, binding.vars.size()));
                }
                var boundRequest = this.boundRequest;
                if (boundRequest != null) {
                    boundRequest.release();
                    this.boundRequest = null;
                }
            } finally {
                unlock();
            }
        }

        @Override public Vars bindableVars() { return originalQuery.allVars(); }
    }

    private interface ClientStreamNode<B extends Batch<B>>
            extends CompletableBatchQueue<B>, ConnectionHandler<NettyHandler>, ChannelBound {
        int cookie();
        void setInfo(@Nullable String info);
        boolean retry();
    }

    private static final class CancelledAckException extends FSException {
        public CancelledAckException() {super("dummy for NettyHttpHandler.onCancelled()");}
    }
    private static final CancelledAckException CANCELLED_ACK = new CancelledAckException();

    private final class NettyHandler extends NettyHttpHandler {
        private static final Logger log = LoggerFactory.getLogger(NettyHandler.class);
        private @Nullable ClientStreamNode<?> downstream;
        private @Nullable ResultsParser<?> parser;
        private @Nullable Charset decodeCS;
        private ByteBufRopeView bbView = new ByteBufRopeView(new SegmentRopeView());
        private @Nullable MutableRope decodeTmp;
        private @Nullable String info;

        public NettyHandler() { super(netty.executor()); }

        public void attach(ClientStreamNode<?> downstream) {
            this.downstream = downstream;
            if (parser != null) {
                if (parser.batchType().equals(downstream.batchType())) {
                    forceReset(parser, downstream);
                } else {
                    parser.release();
                    parser = null;
                }
            }
        }

        private void completeDownstream(@Nullable Throwable cause) {
            journal(cause == null ? "complete" : cause, downstream, "from", this);
            FSException fse = cause == null ? null : FSException.wrap(endpoint, cause);
            if (fse != null) {
                fse.id("handler", journalName());
                if (info != null)
                    fse.id("info", info);
            }
            if (parser != null) {
                if (fse == CANCELLED_ACK)
                    parser.feedCancelledAck();
                else if (fse == null)
                    parser.feedEnd();
                else
                    parser.feedError(fse);
            } else if (downstream != null) {
                if (fse == CANCELLED_ACK)
                    downstream.cancel(true);
                else
                    downstream.complete(fse);
            } else {
                journal("no downstream to deliver ", cause, "on", this);
                log.error("{}: no downstream to deliver err={}", this,
                          cause == null ? null : cause.getClass().getSimpleName(), cause);
            }
            downstream = null;
            if (decodeTmp != null)
                decodeTmp.close();
        }

        /* --- --- --- ChannelBound --- --- --- */

        @Override public String toString() {
            var sb = new StringBuilder().append(super.toString());
            sb.append("down=").append(downstream == null ? "null" : downstream.journalName());
            if (info != null)
                sb.append(", info=").append(info);
            return sb.append('}').toString();
        }

        /* --- --- --- NettyHttpHandler events --- --- --- */

        @Override protected void onSuccessResponse(HttpResponse response) {
            HttpHeaders headers = response.headers();
            info = headers.get("x-fastersparql-info");
            var downstream = requireNonNull(this.downstream);
            downstream.setInfo(info);
            var mt = MediaType.tryParse(headers.get(CONTENT_TYPE));
            if (mt == null)
                throw new InvalidSparqlResultsException("No Content-Type in HTTP response");
            var cs = mt.charset(UTF_8);
            decodeCS = cs == null || cs.equals(UTF_8) || cs.equals(US_ASCII) ? null : cs;
            if (parser == null || !parser.format().asMediaType().accepts(mt)) {
                if (parser != null)
                    parser.release();
                parser = ResultsParser.createFor(fromMediaType(mt), downstream);
                parser.namer(PARSER_NAMER, this);
            }
        }
        private static <B1 extends Batch<B1>, B2 extends Batch<B2>>
        void forceReset(ResultsParser<B1> parser, CompletableBatchQueue<B2> dst) {
            //noinspection unchecked
            parser.reset((CompletableBatchQueue<B1>)dst);
        }
        private static final ResultsParser.Namer<NettyHandler> PARSER_NAMER = (p, h) -> {
            if (p == null) return "null";
            var sb = new StringBuilder().append(p.format().lowercase());
            sb .append(':').append(h.journalName());
            if (h.info != null)
                sb.append("<-").append(h.info);
            return sb.toString();
        };

        @Override protected void onContent(HttpContent content) {
            var parser = requireNonNull(this.parser);
            try {
                var bb = content.content();
                if (decodeCS == null)
                    parser.feedShared(bbView.wrapAsSingle(bb));
                else
                    feedDecoded(parser, bb); // cold
            } catch (TerminatedException | CancelledException e) {
                journal("parser already terminated, err=", e, "on", this);
                cancel(requireNonNull(downstream).cookie());
            }
        }

        private void feedDecoded(ResultsParser<?> parser,
                                 ByteBuf bb) throws CancelledException, TerminatedException {
            if (decodeTmp == null) decodeTmp = new MutableRope(bb.readableBytes());
            else                   decodeTmp.clear();
            parser.feedShared(decodeTmp.append(bb.toString(decodeCS)));
        }

        @Override
        protected void onFailureResponse(@Nullable HttpResponse response, @Nullable MutableRope body, boolean bodyComplete) {
            String msg;
            if (response == null) {
                if (downstream != null && downstream.retry())
                    return; // handled
                msg = "server closed connection without a response";
            } else {
                info = response.headers().get("x-fastersparql-info");
                if (downstream != null)
                    downstream.setInfo(info);
                var sb = new StringBuilder();
                sb.append("HTTP ").append(response.status().code()).append(": ");
                if (body == null || body.len == 0) {
                    sb.append("(no response body)");
                } else {
                    sb.append(body.toString(0, Math.min(256, body.len)).replace("\n", "\\n"));
                    if (body.len > 256)
                        sb.append("... (").append(body.len).append(" bytes)");
                    if (!bodyComplete)
                        sb.append(" (connection closed before completed)");
                }
                msg = sb.toString();
            }
            completeDownstream(new FSServerException(endpoint, msg));
        }

        @Override protected void onClientSideError(Throwable cause) {
            completeDownstream(cause);
        }

        @Override protected void onSuccessLastContent() {
            completeDownstream(null);
        }

        @Override protected void onCancelled(boolean empty) {
            completeDownstream(CANCELLED_ACK);
        }

        @Override protected void onIncompleteSuccessResponse(boolean empty) {
            if (empty && downstream != null && downstream.retry())
                return; // handled
            completeDownstream(new InvalidSparqlResultsException(empty
                    ? "empty SPARQL response"
                    : "server closed connection before completing SPARQL results"));
        }

        /* --- --- --- netty events --- --- --- */

        @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);
            bbView.close();
            bbView = null;
        }
    }
}
