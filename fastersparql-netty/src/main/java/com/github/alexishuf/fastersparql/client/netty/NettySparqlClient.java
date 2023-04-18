package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpHandler;
import com.github.alexishuf.fastersparql.client.netty.util.NettySPSCBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.rope.BufferRope;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParserBIt;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.fromMediaType;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
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
            Supplier<NettyHandler> handlerFac = NettyHandler::new;
            this.netty = new NettyClientBuilder().buildHTTP(ep.uri(), handlerFac);
        } catch (Throwable t) {
            throw FSException.wrap(ep, t);
        }
    }

    @Override public <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sp) {
        if (sp.isGraph())
            throw new FSInvalidArgument("query() method only takes SELECT/ASK queries");
        try {
            return new QueryBIt<>(batchType, sp) ;
        } catch (Throwable t) { throw FSException.wrap(endpoint, t); }
    }

    @Override public void close() { netty.close(); }

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
    private class QueryBIt<B extends Batch<B>> extends NettySPSCBIt<B> {
        private final FullHttpRequest request;
        private @Nullable Charset decodeCharset;
        private @MonotonicNonNull ResultsParserBIt<B> parser;
        private final BufferRope bufferRope = new BufferRope(ByteBuffer.wrap(ByteRope.EMPTY.utf8));

        public QueryBIt(BatchType<B> batchType, SparqlQuery query) {
            super(batchType, query.publicVars(), FSProperties.queueMaxBatches());
            this.request = createRequest(query);
            request();
        }

        /* --- --- --- NettySPSCBIt and request methods --- --- --- */

        @Override public SparqlClient client() { return NettySparqlClient.this; }
        @Override protected void request() {
            // netty will release() after request is written. We must retain() for retries
            request.retain();
            netty.request(request, this::connected, this::complete);
        }

        @Override protected void cleanup(@Nullable Throwable cause) {
            super.cleanup(cause);
            request.release();
        }

        private void connected(Channel ch, NettyHandler handler) {
            this.channel = ch;
            handler.setup(this);
        }

        /* --- --- --- BIt methods --- --- --- */

        @Override public void complete(@Nullable Throwable error) {
            if (!terminated && parser != null) parser.complete(error);
            if (!terminated)                   super.complete(error);
        }

        /* --- --- --- parsing methods called from NettyHandler --- --- --- */

        /**
         * Called once for the {@link HttpResponse}, before the
         * {@link #readContent(HttpContent)} calls start.
         */
        public void startResponse(MediaType mt, Charset cs) {
            if (!cs.equals(UTF_8) && !cs.equals(US_ASCII))
                decodeCharset = cs;
            parser = ResultsParserBIt.createFor(fromMediaType(mt), this);
        }

        /** Called for every {@link HttpContent}, which includes a {@link FullHttpResponse} */
        public void readContent(HttpContent httpContent) {
            Rope r = bufferRope;
            if (decodeCharset == null)
                bufferRope.buffer(httpContent.content().nioBuffer());
            else
                r = new ByteRope(httpContent.content().toString(decodeCharset));
            parser.feedShared(r);
        }
    }


    private final class NettyHandler extends NettyHttpHandler {
        private @Nullable NettySparqlClient.QueryBIt<?> it;
        private int responseBytes = 0;

        @Override public String toString() {
            return NettySparqlClient.this+(ctx == null ? "[UNREGISTERED]"
                                                       : ctx.channel().toString());
        }

        @Override protected void successResponse(HttpResponse resp) {
            if (it == null)
                throw new FSException(endpoint, "HttpResponse received before setup()");
            var mt = MediaType.tryParse(resp.headers().get(CONTENT_TYPE));
            if (mt == null)
                throw new InvalidSparqlResultsException(endpoint, "No Content-Type in HTTP response");
            var cs = mt.charset(UTF_8);
            it.startResponse(mt, cs);
        }

        @Override protected void error(Throwable cause) {
            if (it == null)
                log.error("{}: error() before setup()", this, cause);
            it.complete(cause);
        }

        public void setup(QueryBIt<?> it) {
            responseBytes = 0;
            this.it = it;
            expectResponse();
        }

        @Override protected void content(HttpContent content) {
            if (it == null) {
                log.error("{}: content({}) before setup()", this, content);
            } else {
                responseBytes += content.content().readableBytes();
                it.readContent(content);
                if (content instanceof LastHttpContent) {
                    if (responseBytes == 0)
                        it.complete(new InvalidSparqlResultsException("Zero-byte results"));
                    it.complete(null);
                }
            }
        }
    }
}
