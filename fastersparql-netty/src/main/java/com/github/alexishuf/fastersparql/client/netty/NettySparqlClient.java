package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchQueue.CancelledException;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpHandler;
import com.github.alexishuf.fastersparql.client.netty.util.ByteBufRopeView;
import com.github.alexishuf.fastersparql.client.netty.util.NettyCallbackEmitter;
import com.github.alexishuf.fastersparql.client.netty.util.NettySPSCBIt;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.Future;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.fromMediaType;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.lang.System.identityHashCode;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NettySparqlClient extends AbstractSparqlClient {
    private static final Logger log = LoggerFactory.getLogger(NettySparqlClient.class);

    private static final Set<SparqlMethod> SUPPORTED_METHODS
            = Set.of(SparqlMethod.GET, SparqlMethod.FORM, SparqlMethod.POST);
    private final NettyHttpClient netty;

    public NettySparqlClient(SparqlEndpoint ep) {
        super(withSupported(ep, SUPPORTED_METHODS));
        try {
            Supplier<NettyHandler> handlerFac = () -> new NettyHandler(this);
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
    protected <B extends Batch<B>> Emitter<B>
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
        var method = method(cfg, qry.sparql().len());
        Rope body = switch (method) {
            case POST -> qry.sparql();
            case FORM -> formString(qry.sparql(), cfg.params());
            case GET  -> null;
            default   -> throw new FSInvalidArgument(method+" not supported by "+this);
        };
        String pathAndParams = firstLine(endpoint, cfg, qry.sparql());
        String accept        = qry.isGraph() ? rdfAcceptString(cfg.rdfAccepts())
                                           : resultsAcceptString(cfg.resultsAccepts());
        if (body == null)
            return NettyHttpClient.makeGet(pathAndParams, accept);
        return NettyHttpClient.makeRequest(method2netty(method), pathAndParams, accept,
                                           method.contentType(), body, UTF_8);
    }

    /* --- --- --- inner classes --- --- ---  */

    /** Asynchronous {@code BIt} that sends an HTTP request on first access
     *  and is fed with results from the server.
     *
     *  <p>This object is distinct from {@link NettyHandler} since it is unique to a query,
     *  while {@link NettyHandler}'s lifecycle is attached to the Netty channel which may be
     *  reused for multiple SPARQL queries.</p>
     */
    private class QueryBIt<B extends Batch<B>> extends NettySPSCBIt<B>
            implements NettyHttpClient.ConnectionHandler<NettyHandler> {
        private final FullHttpRequest request;
        private @Nullable NettyHandler handler;

        public QueryBIt(BatchType<B> batchType, SparqlQuery query) {
            super(batchType, query.publicVars(), FSProperties.queueMaxRows(),
                  NettySparqlClient.this);
            this.request = createRequest(query);
            acquireRef();
            request();
        }

        @Override public String journalName() {
            return "C.QB:" + (lastChannel == null ? "null" : lastChannel.id().asShortText());
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            try { super.cleanup(cause); } catch (Throwable t) { reportCleanupError(t); }
            try { releaseRef();         } catch (Throwable t) { reportCleanupError(t); }
            try { request.release();    } catch (Throwable t) { reportCleanupError(t); }
        }

        @Override protected void request() { netty.connect(this); }

        @Override public void operationComplete(Future<?> future) {
            netty.handleChannel(future, this);
        }

        @Override public HttpRequest httpRequest() { return request.retain(); }

        @Override public void onConnected(Channel ch, NettyHandler handler) {
            this.channel     = ch;
            this.lastChannel = ch;
            lock();
            try {
                if (canSendRequest()) {
                    this.handler = handler;
                    handler.setup(this);
                } else {
                    this.handler = null;
                    throw NettyHttpClient.ABORT_REQUEST;
                }
            } finally { unlock(); }
        }

        @Override public void onConnectionError(Throwable cause) { complete(cause); }

        @Override protected boolean cancelAfterRequestSent() {
            assert handler != null : "request sent but handler == null";
            handler.cancelAndClose(this);
            return false;
        }
    }

    private final class QueryEmitter<B extends Batch<B>> extends NettyCallbackEmitter<B>
            implements NettyHttpClient.ConnectionHandler<NettyHandler> {
        private final SparqlQuery originalQuery;
        private SparqlQuery boundQuery;
        private @Nullable FullHttpRequest boundRequest;
        private @Nullable B lb;
        private @Nullable BatchMerger<B> merger;
        private @Nullable Vars mergerFreeVars;
        private @Nullable NettyHandler handler;

        public QueryEmitter(BatchType<B> batchType, SparqlQuery query) {
            super(batchType, query.publicVars(), NettySparqlClient.this);
            this.originalQuery = query;
            this.boundQuery    = query;
            acquireRef();
        }

        @Override public String journalName() {
            return String.format("C.QE@%x", System.identityHashCode(this));
        }

        @Override protected void doRelease() {
            var boundRequest = this.boundRequest;
            if (boundRequest != null) {
                boundRequest.release();
                this.boundRequest = null;
            }
            lb = batchType().recycle(lb);
            if (this.merger != null) {
                this.merger.release();
                this.merger = null;
            }
            releaseRef();
        }

        @Override public int preferredRequestChunk() {
            return 8*super.preferredRequestChunk();
        }

        @Override protected void request() {
            handler = null;
            netty.connect(this);
        }

        @Override public void operationComplete(Future<?> future) {
            netty.handleChannel(future, this);
        }

        @Override public HttpRequest httpRequest() {
            var request = this.boundRequest;
            if (request == null)
                this.boundRequest = request = createRequest(boundQuery);
            request.retain(); // retain for retries, Nett will release() on write()
            return request;
        }

        @Override public void onConnected(Channel ch, NettyHandler handler) {
            if (ThreadJournal.ENABLED)
                journal("connected em=", this, "ch=", ch.toString());
            setChannel(ch);
            int st = lock(statePlain());
            try {
                if (canSendRequest()) {
                    this.handler = handler;
                    handler.setup(this);
                } else {
                    throw NettyHttpClient.ABORT_REQUEST; // recycle ch
                }
            } finally { unlock(st); }
        }

        @Override public void onConnectionError(Throwable cause) { complete(cause); }

        @Override protected boolean cancelAfterRequestSent() {
            assert handler != null : "REQUEST_SENT but handler == null";
            handler.cancelAndClose(this);
            return false; // do TaskEmitter.cancel()
        }

        @Override protected @Nullable B deliver(B b) {
            if (merger == null)
                b = super.deliver(b);
            else
                bt.recycle(super.deliver(merger.merge(null, lb, 0, b)));
            return b;
        }

        @Override public void rebind(BatchBinding binding) throws RebindException {
            int st = resetForRebind(REBIND_CLEAR, LOCKED_MASK);
            try {
                boundQuery = originalQuery.bound(binding);
                var freeVars = boundQuery.publicVars();
                if (freeVars.size() == originalQuery.publicVars().size()) {
                    lb = bt.recycle(lb);
                    merger = null;
                } else {
                    if (!freeVars.equals(mergerFreeVars)) {
                        mergerFreeVars = freeVars;
                        merger          = bt.merger(vars, binding.vars, vars);
                    }
                    binding.putRow(lb = bt.empty(lb, binding.vars.size()));
                }
                var boundRequest = this.boundRequest;
                if (boundRequest != null) {
                    boundRequest.release();
                    this.boundRequest = null;
                }
            } finally {
                unlock(st);
            }
        }

        @Override public Vars bindableVars() { return originalQuery.allVars(); }
    }

    private static final class NettyHandler extends NettyHttpHandler {
        private CompletableBatchQueue<?> downstream;
        private @MonotonicNonNull ResultsParser<?> parser;
        private @Nullable Charset decodeCS;
        private final ByteBufRopeView bbRopeView = ByteBufRopeView.create();
        private int resBytes = 0;
        private final NettySparqlClient sparqlClient;

        private NettyHandler(NettySparqlClient sparqlClient) {
            this.sparqlClient = sparqlClient;
        }

        @Override public String toString() {
            return String.format("{ch=%s, %s, resBytes=%d}@%x",
                    ctx == null ? null : ctx.channel(), isTerminated() ? "term" : "!term",
                    resBytes, identityHashCode(this));
        }

        @Override public String journalName() {
            return "C.NH:" + (ctx == null ? "null" : ctx.channel().id().asShortText());
        }

        @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            journal("channelUnregistered on", this);
            bbRopeView.recycle();
            super.channelUnregistered(ctx);
        }

        public void setup(CompletableBatchQueue<?> downstream) {
            journal("setup nettyHandler=", this, "down=", downstream);
            resBytes = 0;
            this.downstream = downstream;
            expectResponse();
        }

        public void cancelAndClose(CompletableBatchQueue<?> downstream) {
            if (downstream == this.downstream && markTerminated()) {
                journal("cancel-induced ctx.close() on ", this);
                ctx.close();
            }
        }

        @Override protected void successResponse(HttpResponse resp) {
            if (downstream == null)
                throw new FSException(sparqlClient.endpoint, "HttpResponse received before setup()");
            var mt = MediaType.tryParse(resp.headers().get(CONTENT_TYPE));
            if (mt == null)
                throw new InvalidSparqlResultsException(sparqlClient.endpoint, "No Content-Type in HTTP response");
            var cs = mt.charset(UTF_8);
            decodeCS = cs == null || cs.equals(UTF_8) || cs.equals(US_ASCII) ? null : cs;
            parser = ResultsParser.createFor(fromMediaType(mt), downstream);
        }

        @Override protected void error(Throwable cause) {
            journal("error on", this, ": ", cause);
            FSException ex = FSException.wrap(sparqlClient.endpoint, cause);
            if (parser != null)
                parser.feedError(ex);
            if (downstream == null)
                log.error("{}: error({}) before setup", this, cause, cause);
            downstream.complete(ex);
        }

        @Override protected void content(HttpContent content) {
            var parser = this.parser;
            if (parser == null) {
                log.error("{}: content({}) before setup()/successResponse()", this, content);
                return;
            }
            ByteBuf bb = content.content();
            resBytes += bb.readableBytes();
            try {
                parser.feedShared(decodeCS == null ? bbRopeView.wrapAsSingle(bb)
                                                   : new ByteRope(bb.toString(decodeCS)));
                if (content instanceof LastHttpContent) {
                    if (ThreadJournal.ENABLED)
                        journal("LastHttpContent on", this, "ch=", channel());
                    if (resBytes == 0)
                        error(new InvalidSparqlResultsException("Zero-byte results"));
                    else
                        parser.feedEnd();
                }
            } catch (TerminatedException|CancelledException e) {
                journal("abort content() on ", this, "due to down term|cancel");
                ctx.close();
            }
        }
    }
}
