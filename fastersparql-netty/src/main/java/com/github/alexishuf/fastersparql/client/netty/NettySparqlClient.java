package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.AbstractSparqlClient;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpHandler;
import com.github.alexishuf.fastersparql.client.netty.util.NettySPSCBufferedBIt;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSInvalidArgument;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.MediaType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.BufferRope;
import com.github.alexishuf.fastersparql.model.rope.ByteRope;
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.model.row.NotRowType;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParserBIt;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static com.github.alexishuf.fastersparql.model.SparqlResultFormat.fromMediaType;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NettySparqlClient extends AbstractSparqlClient {

    private static final Set<SparqlMethod> SUPPORTED_METHODS
            = Set.of(SparqlMethod.GET, SparqlMethod.FORM, SparqlMethod.POST);
    private final AtomicInteger nextHandlerId = new AtomicInteger(1);
    private final NettyHttpClient netty;

    public NettySparqlClient(SparqlEndpoint ep) {
        super(withSupported(ep, SUPPORTED_METHODS));
        try {
            Supplier<NettyHandler> handlerFac = () -> new NettyHandler(nextHandlerId.getAndIncrement());
            this.netty = new NettyClientBuilder().buildHTTP(ep.uri(), handlerFac);
        } catch (Throwable t) {
            throw FSException.wrap(ep, t);
        }
    }

    @Override public <R> BIt<R> query(RowType<R> rowType, SparqlQuery sp) {
        if (sp.isGraph())
            throw new FSInvalidArgument("query() method only takes SELECT/ASK queries");
        try {
            return new QueryBIt<>(sp, rowType);
        } catch (Throwable t) { throw FSException.wrap(endpoint, t); }
    }

    @Override
    public Graph queryGraph(SparqlQuery sp) {
        if (!sp.isGraph())
            throw new FSInvalidArgument("query() method only takes CONSTRUCT/DESCRIBE queries");
        try {
            GraphBIt bit = new GraphBIt(sp);
            return new Graph(bit.mediaTypeFuture, bit);
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

    private HttpRequest createRequest(SparqlQuery qry) {
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
    private abstract class ClientBIt<T> extends NettySPSCBufferedBIt<T> {
        private final HttpRequest request;

        public ClientBIt(RowType<T> rowType, Vars vars, HttpRequest request) {
            super(rowType, vars);
            this.request = request;
        }

        @Override public SparqlClient client() { return NettySparqlClient.this; }
        @Override protected void request() { netty.request(request, this::connected, this::complete); }

        private void connected(Channel ch, NettyHandler handler) {
            lock();
            try {
                this.channel = ch;
                handler.setup(this);
            } finally { unlock(); }
        }

        /** Called once for the {@link FullHttpResponse} or the first {@link HttpContent}. */
        public abstract void startResponse(MediaType mediaType, Charset charset);
        /** Called for every {@link HttpContent}, which includes a {@link FullHttpResponse} */
        public abstract void readContent(HttpContent httpContent);
    }

    /** {@link ClientBIt} for ASK/SELECT queries */
    private class QueryBIt<R> extends ClientBIt<R> {
        private @Nullable Charset decodeCharset;
        private @MonotonicNonNull ResultsParserBIt<R> parser;
        private final BufferRope bufferRope = new BufferRope(ByteBuffer.wrap(ByteRope.EMPTY.utf8));

        public QueryBIt(SparqlQuery sparql, RowType<R> rowType) {
            super(rowType, sparql.publicVars(), createRequest(sparql));
        }

        /* --- --- --- BIt methods --- --- --- */

        @Override public void complete(@Nullable Throwable error) {
            if (!terminated && parser != null) parser.complete(error);
            if (!terminated)                   super.complete(error);
        }

        /* --- --- --- ClientBIt message-parsing methods --- --- --- */

        @Override
        public void startResponse(MediaType mt, Charset cs) {
            if (!cs.equals(UTF_8) && !cs.equals(US_ASCII))
                decodeCharset = cs;
            parser = ResultsParserBIt.createFor(fromMediaType(mt), rowType, this);
        }

        @Override public void readContent(HttpContent httpContent) {
            Rope r = bufferRope;
            if (decodeCharset == null)
                bufferRope.buffer = httpContent.content().nioBuffer();
            else
                r = new ByteRope(httpContent.content().toString(decodeCharset));
            parser.feedShared(r);
        }
    }

    /** {@link ClientBIt} for fragments (i.e., {@link Graph}) results */
    private class GraphBIt extends ClientBIt<byte[]> {
        final CompletableFuture<MediaType> mediaTypeFuture = new CompletableFuture<>();

        public GraphBIt(SparqlQuery sparql) {
            super(NotRowType.BYTES, Vars.EMPTY, createRequest(sparql));
        }

        /* --- --- --- ClientBIt message-parsing methods --- --- --- */

        @Override
        public void startResponse(MediaType mediaType, Charset charset) {
            mediaTypeFuture.complete(mediaType);
        }

        @Override public void readContent(HttpContent httpContent) {
            ByteBuf bb = httpContent.content();
            byte[] bytes = new byte[bb.readableBytes()];
            bb.readBytes(bytes);
            feed(bytes);
        }
    }

    private final class NettyHandler extends NettyHttpHandler {
        private final int id;
        private @Nullable ClientBIt<?> it;
        private @Nullable Throwable earlyFailure;

        public NettyHandler(int id) {
            this.id = id;
        }
        @Override public String toString() {return NettySparqlClient.this+"-NettyHandler-"+id;}

        private void complete(@Nullable Throwable error) {
            if (it == null) {
                if (error == null)
                    error = new IllegalStateException("NettyHandler completing before setup()");
                if (earlyFailure == null) earlyFailure = error;
                else                      earlyFailure.addSuppressed(error);
            } else {
                it.complete(error == null ? earlyFailure : error);
                it = null;
                earlyFailure = null;
                recycle();
            }
        }

        public void setup(ClientBIt<?> msgHandler) {
            this.it = msgHandler;
            if (earlyFailure != null)
                complete(earlyFailure);
            else if (!ctx.channel().isOpen())
                complete(new FSServerException("Could not connect or server closed before request was sent"));
        }

        @Override protected void response(HttpResponse resp) {
            if (it == null)
                throw new FSException(endpoint, "HttpResponse received before setup()");
            var mt = MediaType.tryParse(resp.headers().get(CONTENT_TYPE));
            var cs = mt == null ? UTF_8 : mt.charset(UTF_8);

            HttpStatusClass codeClass = resp.status().codeClass();
            if (codeClass == HttpStatusClass.REDIRECTION) {
                String errMsg = "NettyBItSparqlClient does not support redirection";
                throw new FSServerException(errMsg);
            } else if (codeClass != HttpStatusClass.SUCCESS) {
                var errMsg = "Request failed with "+resp.status();
                if (resp instanceof HttpContent httpContent)
                    errMsg += ": " + httpContent.content().toString(cs);
                var ex = new FSServerException(errMsg);
                throw ex.shouldRetry(codeClass == HttpStatusClass.SERVER_ERROR);
            } else if (resp instanceof LastHttpContent hc && hc.content().readableBytes() == 0) {
                String errMsg = "Empty HTTP " + resp.status();
                throw new FSServerException(errMsg).shouldRetry(true);
            }
            if (mt == null)
                throw new InvalidSparqlResultsException(endpoint, "No Content-Type in HTTP response");
            else if (it == null)
                throw new FSException(endpoint, "HTTP response received before setup()");
            it.startResponse(mt, cs);
        }

        @Override protected void content(HttpContent content) {
            assert it != null;
            it.readContent(content);
        }

        @Override protected void responseEnd(Throwable error) {
            ctx.channel().config().setAutoRead(true);
            if (error == null && responseBytes == 0)
                error = new InvalidSparqlResultsException("Empty HTTP response");
            complete(error);
        }
    }
}
