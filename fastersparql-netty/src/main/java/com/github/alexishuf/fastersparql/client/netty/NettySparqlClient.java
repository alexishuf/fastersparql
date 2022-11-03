package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientException;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientInvalidArgument;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.SparqlMethod;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.util.NettyCallbackBIt;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.client.parser.results.ResultsParser;
import com.github.alexishuf.fastersparql.client.parser.results.ResultsParserConsumer;
import com.github.alexishuf.fastersparql.client.parser.results.ResultsParserRegistry;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.bind.ClientBindingBIt;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NettySparqlClient<R, I, F> extends AbstractNettySparqlClient<R, I, F> {
    private static final Set<SparqlMethod> SUPPORTED_METHODS
            = Set.of(SparqlMethod.GET, SparqlMethod.FORM, SparqlMethod.POST);
    private final AtomicInteger nextHandlerId = new AtomicInteger(1);
    private final NettyHttpClient netty;

    public NettySparqlClient(SparqlEndpoint ep, RowType<R, I> rowType,
                             FragmentParser<F> fragParser) {
        super(withSupported(ep, ResultsParserRegistry.get(), SUPPORTED_METHODS),
                rowType, fragParser);
        try {
            Supplier<NettyHandler> handlerFac = () -> new NettyHandler(nextHandlerId.getAndIncrement());
            this.netty = new NettyClientBuilder().buildHTTP(ep.uri(), handlerFac);
        } catch (Throwable t) {
            throw SparqlClientException.wrap(ep, t);
        }
    }

    @Override protected String endpointString() {
        var reg = ResultsParserRegistry.get();
        var cfg = this.endpoint.configuration();
        //noinspection SlowListContainsAll
        boolean trivial = cfg.methods().containsAll(SUPPORTED_METHODS) &&
                cfg.resultsAccepts().stream().allMatch(fmt -> reg.canParse(fmt.asMediaType()));
        return trivial ? endpoint.uri() : endpoint.toString();
    }

    @Override
    public BIt<R> query(SparqlQuery sparql, @Nullable BIt<R> bindings, @Nullable BindType bindType) {
        if (bindings == null || bindings instanceof EmptyBIt<R>)
            return query(sparql);
        else if (bindType == null)
            throw new NullPointerException("bindings != null, but bindType is null!");
        if (sparql.isGraph)
            throw new InvalidSparqlQueryType("query() method only takes SELECT/ASK queries");
        try {
            return new ClientBindingBIt<>(bindings, bindType, rowType, this,
                                          sparql);
        } catch (Throwable t) {
            throw SparqlClientException.wrap(endpoint, t);
        }
    }

    @Override
    public BIt<R> query(SparqlQuery sp) {
        if (sp.isGraph)
            throw new SparqlClientInvalidArgument("query() method only takes SELECT/ASK queries");
        try {
            return new QueryBIt(sp);
        } catch (Throwable t) { throw SparqlClientException.wrap(endpoint, t); }
    }

    @Override
    public Graph<F> queryGraph(SparqlQuery sp) {
        if (!sp.isGraph)
            throw new SparqlClientInvalidArgument("query() method only takes CONSTRUCT/DESCRIBE queries");
        try {
            GraphBIt bit = new GraphBIt(sp);
            return new Graph<>(bit.mediaTypeFuture, bit);
        } catch (Throwable t) { throw SparqlClientException.wrap(endpoint, t); }
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
        var method = method(cfg, qry.sparql.length());
        CharSequence body = switch (method) {
            case POST -> qry.sparql;
            case FORM -> formString(qry.sparql, cfg.params());
            case GET  -> null;
            default   -> throw new SparqlClientInvalidArgument(method+" not supported by "+this);
        };
        String pathAndParams = firstLine(endpoint, cfg, qry.sparql).toString();
        String accept        = qry.isGraph ? rdfAcceptString(cfg.rdfAccepts())
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
    private abstract class ClientBIt<T> extends NettyCallbackBIt<T> {
        private final HttpRequest request;

        public ClientBIt(Class<T> eCls, Vars vars, HttpRequest request) {
            super(eCls, vars);
            this.request = request;
        }

        @Override public SparqlClient<?, ?, ?> client() { return NettySparqlClient.this; }
        @Override protected void request() { netty.request(request, this::connected, this::complete); }

        private void connected(Channel ch, NettyHandler handler) {
            lock.lock();
            try {
                this.channel = ch;
                handler.setup(this);
            } finally { lock.unlock(); }
        }

        /** Called once for the {@link FullHttpResponse} or the first {@link HttpContent}. */
        public abstract void startResponse(MediaType mediaType, Charset charset);
        /** Called for every {@link HttpContent}, which includes a {@link FullHttpResponse} */
        public abstract void readContent(HttpContent httpContent);
    }

    /** {@link ClientBIt} for ASK/SELECT queries */
    private class QueryBIt extends ClientBIt<R> implements ResultsParserConsumer {
        private @MonotonicNonNull Charset charset;
        private @MonotonicNonNull ResultsParser parser;
        private boolean ignoreParserError;
        private Merger<R, I> projector;
        private Function<String[], R> converter;

        public QueryBIt(SparqlQuery sparql) {
            super(rowClass(), sparql.publicVars, createRequest(sparql));
            projector = Merger.identity(rowType, sparql.publicVars);
            converter = rowType.converter(ArrayRow.STRING, vars);
        }

        /* --- --- --- BIt methods --- --- --- */

        @Override public void complete(@Nullable Throwable error) {
            if (!ended && parser != null) {
                ignoreParserError = true;
                parser.end();
            }
            super.complete(error);
        }

        /* --- --- --- ClientBIt message-parsing methods --- --- --- */

        @Override
        public void startResponse(MediaType mt, Charset cs) {
            charset = cs;
            parser = ResultsParserRegistry.get().createFor(mt, this);
        }

        @Override public void readContent(HttpContent httpContent) {
            parser.feed(httpContent.content().toString(charset));
        }

        /* --- --- --- ResultsParserConsumer methods --- --- --- */

        @Override public void end()                                  { /* end on response end */ }
        @Override public void onError(String message)                { if (!ignoreParserError) complete(new InvalidSparqlResultsException(endpoint, message)); }
        @Override public void row(@Nullable String[] row)            { feed(projector.merge(converter.apply(row), null)); }
        @Override public void vars(Vars vars)                {
            converter = rowType.converter(ArrayRow.STRING, vars);
            projector = Merger.forProjection(rowType, projector.outVars(), vars);
        }
    }

    /** {@link ClientBIt} for fragments (i.e., {@link Graph}) results */
    private class GraphBIt extends ClientBIt<F> {
        private @MonotonicNonNull Function<ByteBuf, F> parser;
        final CompletableFuture<MediaType> mediaTypeFuture = new CompletableFuture<>();

        public GraphBIt(SparqlQuery sparql) {
            super(fragmentClass(), Vars.EMPTY, createRequest(sparql));
        }

        /* --- --- --- ClientBIt message-parsing methods --- --- --- */

        @Override
        public void startResponse(MediaType mediaType, Charset charset) {
            mediaTypeFuture.complete(mediaType);
            Class<? super F> fragClass = fragParser.fragmentClass();
            if (fragClass.isAssignableFrom(String.class)) {
                parser = bb -> fragParser.parseString(bb.toString(charset), charset);
            } else {
                parser = bb -> {
                    byte[] bytes = new byte[bb.readableBytes()];
                    bb.readBytes(bytes);
                    return fragParser.parseBytes(bytes, charset);
                };
            }
        }

        @Override public void readContent(HttpContent httpContent) {
            feed(parser.apply(httpContent.content()));
        }
    }

    private final class NettyHandler extends SimpleChannelInboundHandler<HttpObject>
            implements ReusableHttpClientInboundHandler {
        private final int id;
        private boolean gotResponse;
        private @MonotonicNonNull Runnable onResponseEnd;
        private @Nullable ClientBIt<?> it;
        private @Nullable Throwable earlyFailure;

        public NettyHandler(int id) {
            this.id = id;
        }
        @Override public String toString() {return NettySparqlClient.this+"-NettyHandler-"+id;}
        @Override public void onResponseEnd(Runnable runnable) { onResponseEnd = runnable; }

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
                gotResponse = false;
                if (onResponseEnd != null) onResponseEnd.run();
            }
        }

        public void setup(ClientBIt<?> msgHandler) {
            this.it = msgHandler;
            if (earlyFailure != null)
                complete(earlyFailure);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (!gotResponse && msg instanceof HttpResponse resp) {
                gotResponse = true;
                var mt = MediaType.tryParse(resp.headers().get(CONTENT_TYPE));
                var cs = mt == null ? UTF_8 : mt.charset(UTF_8);

                HttpStatusClass codeClass = resp.status().codeClass();
                if (codeClass == HttpStatusClass.REDIRECTION) {
                    String errMsg = "NettyBItSparqlClient does not support redirection";
                    throw new SparqlClientServerException(errMsg);
                } else if (codeClass != HttpStatusClass.SUCCESS) {
                    var errMsg = "Request failed with "+resp.status();
                    if (resp instanceof HttpContent httpContent)
                        errMsg += ": " + httpContent.content().toString(cs);
                    var ex = new SparqlClientServerException(errMsg);
                    throw ex.shouldRetry(codeClass == HttpStatusClass.SERVER_ERROR);
                } else if (msg instanceof LastHttpContent hc && hc.content().readableBytes() == 0) {
                    String errMsg = "Empty HTTP " + resp.status();
                    throw new SparqlClientServerException(errMsg).shouldRetry(true);
                }
                if (mt == null)
                    throw new InvalidSparqlResultsException(endpoint, "No Content-Type in HTTP response");
                else if (it == null)
                    throw new SparqlClientException(endpoint, "HTTP response received before setup()");
                it.startResponse(mt, cs);
            } else if (it == null) {
                throw new SparqlClientException(endpoint, "HttpResponse received before setup()");
            }
            if (msg instanceof HttpContent content)
                it.readContent(content);
            if (msg instanceof LastHttpContent httpContent) {
                ctx.channel().config().setAutoRead(true);
                if (msg instanceof HttpResponse && httpContent.content().readableBytes() == 0)
                    complete(new InvalidSparqlResultsException(endpoint, "Empty HTTP response"));
                else
                    complete(null);
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            String msg = "Connection closed before "
                    + (gotResponse ? "completing the response"
                                   : "server started an HTTP response");
            complete(new SparqlClientServerException(msg));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            complete(cause);
        }
    }
}
