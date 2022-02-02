package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClientBuilder;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.results.*;
import com.github.alexishuf.fastersparql.client.parser.row.RowParser;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.SafeAsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import com.github.alexishuf.fastersparql.client.util.reactive.CallbackPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.client.util.sparql.Projector;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;


public class NettySparqlClient<R, F> implements SparqlClient<R, F> {
    private static final Logger log = LoggerFactory.getLogger(NettySparqlClient.class);
    private final SparqlEndpoint endpoint;
    private final AsyncTask<NettyHttpClient<Handler>> netty;
    private final RowParser<R> rowParser;
    private final FragmentParser<F> fragParser;


    public NettySparqlClient(SparqlEndpoint endpoint, RowParser<R> rowParser,
                             FragmentParser<F> fragmentParser) {
        if (endpoint == null) throw new NullPointerException("endpoint is null");
        if (rowParser == null) throw new NullPointerException("rowParser is null");
        if (fragmentParser == null) throw new NullPointerException("fragmentParser is null");
        this.endpoint = withoutUnsupportedResultFormats(endpoint, ResultsParserRegistry.get());
        this.netty = endpoint.resolvedHost().thenApplyThrowing(a ->
                new NettyHttpClientBuilder().build(endpoint.protocol(), a, Handler::new));
        this.rowParser = rowParser;
        this.fragParser = fragmentParser;
    }

    @Override public SparqlEndpoint endpoint() {
        return endpoint;
    }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        List<String> vars = SparqlUtils.publicVars(sparql);
        Throwable cause;
        try {
            SparqlConfiguration eff = effectiveConfig(endpoint, configuration, sparql.length());
            SparqlMethod method = eff.methods().get(0);
            HttpMethod nettyMethod = method2netty(method);
            PublisherAdapter<String[]> publisher = new PublisherAdapter<>();
            netty.get().request(nettyMethod, firstLine(endpoint, eff, sparql),
                    nettyMethod == HttpMethod.GET ? null : a -> generateBody(a, eff, sparql),
                    (ch, request, handler) -> {
                        String accept = resultsAcceptString(eff.resultsAccepts());
                        request.headers().set(HttpHeaderNames.ACCEPT, accept);
                        if (method.hasRequestBody())
                            request.headers().set(CONTENT_TYPE, method.contentType());
                        handler.setupResults(ch, vars, publisher);
                    });
            Results<String[]> raw = new Results<>(vars, String[].class, publisher);
            Publisher<R> parsedPub = rowParser.parseStringsArray(raw);
            if (parsedPub == raw.publisher()) //noinspection unchecked
                return (Results<R>) raw;
            return new Results<>(vars, rowParser.rowClass(), parsedPub);
        } catch (ExecutionException e) {
            cause = e.getCause() == null ? e : e.getCause();
        } catch (Throwable t) {
            cause = t;
        }
        return Results.error(vars, rowParser.rowClass(), cause);
    }

    @Override
    public Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        Throwable cause;
        try {
            SparqlConfiguration eff = effectiveConfig(endpoint, configuration, sparql.length());
            SparqlMethod method = eff.methods().get(0);
            HttpMethod nettyMethod = method2netty(method);
            SafeCompletableAsyncTask<MediaType> mtTask = new SafeCompletableAsyncTask<>();
            PublisherAdapter<byte[]> publisher = new PublisherAdapter<>();
            netty.get().request(nettyMethod, firstLine(endpoint, eff, sparql),
                    nettyMethod == HttpMethod.GET ? null : a -> generateBody(a, eff, sparql),
                    (ch, request, handler) -> {
                        String accept = rdfAcceptString(eff.rdfAccepts());
                        request.headers().set(HttpHeaderNames.ACCEPT, accept);
                        if (method.hasRequestBody())
                            request.headers().set(CONTENT_TYPE, method.contentType());
                        handler.setupGraph(ch, mtTask, publisher);
                    });
            Graph<byte[]> raw = new Graph<>(mtTask, byte[].class, publisher);
            Publisher<F> parsedPub = fragParser.parseBytes(raw);
            if (parsedPub == raw.publisher()) //noinspection unchecked
                return (Graph<F>) raw;
            return new Graph<>(mtTask, fragParser.fragmentClass(), parsedPub);
        } catch (ExecutionException e) {
            cause = e.getCause() == null ? e : e.getCause();
        } catch (Throwable t) {
            cause = t;
        }
        SafeAsyncTask<MediaType> nullMT = Async.wrap((MediaType) null);
        return new Graph<>(nullMT, fragParser.fragmentClass(), new EmptyPublisher<>(cause));
    }

    @Override public void close() {
        boolean cancel = !netty.isDone();
        if (cancel)
            netty.cancel(true);
        NettyHttpClient<Handler> client = null;
        try {
            client = netty.orElse(null);
        } catch (ExecutionException e) {
            if (cancel) {
                log.info("{}.close(): NettyHttpClient construction failed after cancel: ",
                         this, e.getCause());
            } else {
                log.info("{}.close(): NettyHttpClient construction failed: ", this, e.getCause());
            }
        }
        if (client != null)
            client.close();
    }

    /* --- --- --- helper methods  --- --- ---  */

    private static HttpMethod method2netty(SparqlMethod method) {
        switch (method) {
            case GET:
                return HttpMethod.GET;
            case POST:
            case FORM:
                return HttpMethod.POST;
            default:
                throw new IllegalArgumentException("Unexpected SparqlMethod "+ method);
        }
    }

    private ByteBuf generateBody(ByteBufAllocator allocator, SparqlConfiguration config,
                                 CharSequence sparql) {
        CharSequence body;
        switch (config.methods().get(0)) {
            case POST: body = sparql; break;
            case FORM: body = formString(sparql, config.params()); break;
            default: return null;
        }
        ByteBuf bb = allocator.buffer(body.length());
        bb.writeCharSequence(body, UTF_8);
        return bb;
    }

    /* --- --- --- inner classes  --- --- ---  */

    /**
     * The {@link Publisher} exposed by {@link NettySparqlClient} query methods
     * (when no row/fragment parser is used)
     */
    @Setter @Accessors(fluent = true)
    private static class PublisherAdapter<T> extends CallbackPublisher<T> {
        private @MonotonicNonNull Handler handler;
        @Override protected void onRequest(long n) {
            if (handler != null) handler.autoRead(true);
        }
        @Override protected void onBackpressure() {
            if (handler != null) handler.autoRead(false);
        }
        @Override protected void onCancel() {
            if (handler != null) handler.abort();
        }
    }
    /**
     * Listens as a {@link ResultsParserConsumer} and feeds a {@link PublisherAdapter}.
     */
    @RequiredArgsConstructor
    private static class ResultsParserAdapter implements ResultsParserConsumer {
        private final List<String> expectedVars;
        private final PublisherAdapter<String[]> publisher;
        private Projector projector = Projector.IDENTITY;

        @Override public void vars(List<String> vars) {
            projector = Projector.createFor(expectedVars, vars);
        }
        @Override public void row(@Nullable String[] row) {
            publisher.feed(projector.project(row));
        }
        @Override public void end() { publisher.complete(null); }
        @Override public void onError(String message) {
            publisher.complete(new InvalidSparqlResultsException(message));
        }
    }


    private static class Handler extends SimpleChannelInboundHandler<HttpObject>
            implements ReusableHttpClientInboundHandler {
        private static final Logger log = LoggerFactory.getLogger(Handler.class);
        private Runnable onResponseEnd;
        private Channel channel;
        private Throwable failure;
        private ResultsParserAdapter resultsAdapter;
        private PublisherAdapter<byte[]> fragmentPublisher;
        private SafeCompletableAsyncTask<MediaType> mediaTypeTask;
        private ResultsParser resultsParser;
        private MediaType mediaType;
        private Charset charset = UTF_8;

        @Override public void onResponseEnd(Runnable runnable) {
            this.onResponseEnd = runnable;
        }

        protected void responseEnded() {
            synchronized (this) {
                if (resultsParser != null) resultsParser.end();
                if (resultsAdapter != null) resultsAdapter.end();
                if (fragmentPublisher != null) fragmentPublisher.complete(null);
                channel = null;
            }
            if (onResponseEnd != null) onResponseEnd.run();
        }

        private void reset(Channel channel) {
            this.failure = null;
            this.mediaType = null;
            this.charset = null;
            this.fragmentPublisher = null;
            this.mediaTypeTask = null;
            this.resultsParser = null;
            this.resultsAdapter = null;
            this.channel = channel;
        }

        public synchronized void setupResults(Channel channel, List<String> outVars,
                                              PublisherAdapter<String[]> rowPublisher) {
            reset(channel);
            this.channel = channel;
            this.resultsAdapter = new ResultsParserAdapter(outVars, rowPublisher);
            rowPublisher.handler(this);
        }

        public synchronized void setupGraph(Channel channel,
                                            SafeCompletableAsyncTask<MediaType> mediaTypeTask,
                                            PublisherAdapter<byte[]> fragmentPublisher) {
            reset(channel);
            this.channel = channel;
            this.mediaTypeTask = mediaTypeTask;
            (this.fragmentPublisher = fragmentPublisher).handler(this);
        }

        public synchronized void autoRead(boolean value) {
            assert channel != null : "autoRead() before setup()";
            channel.config().setAutoRead(value);
        }

        public synchronized void abort() {
            assert channel != null : "abort() before setup()";
            channel.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
            if (failure != null) {
                if (msg instanceof HttpContent) {
                    log.debug("Chunk after {}: {}", failure.getClass().getSimpleName(),
                              ((HttpContent) msg).content().toString(charset));
                }
                return;
            }
            if (msg instanceof HttpResponse) {
                HttpResponse resp = (HttpResponse) msg;
                mediaType = MediaType.tryParse(resp.headers().get(CONTENT_TYPE));
                if (mediaTypeTask != null)
                    mediaTypeTask.complete(mediaType);
                charset = mediaType == null ? UTF_8 : mediaType.charset(UTF_8);
                String exception = null;
                if (resp.status().codeClass() == HttpStatusClass.REDIRECTION) {
                    exception = "NettySparqlClient does not support redirection";
                } else if (resp.status().codeClass() != HttpStatusClass.SUCCESS) {
                    exception = "Request failed with "+resp.status();
                    if (resp instanceof HttpContent) {
                        exception += ": "+((HttpContent) resp).content().toString(charset);
                    }
                }
                if (exception != null)
                    throw new SparqlClientServerException(exception);
                else if (mediaType == null)
                    throw new InvalidSparqlResultsException("Bad server did not set Content-Type");
            }

            if (resultsAdapter != null) {
                assert fragmentPublisher == null : "both fragment row publishers set";
                readRows(msg);
            } else {
                assert fragmentPublisher != null : "no publisher set";
                readFragments(msg);
            }
            if (msg instanceof LastHttpContent)
                responseEnded();
        }

        private void readRows(HttpObject msg) throws NoParserException {
            if (msg instanceof HttpResponse)
                resultsParser = ResultsParserRegistry.get().createFor(mediaType, resultsAdapter);
            if (msg instanceof HttpContent)
                resultsParser.feed(((HttpContent) msg).content().toString(charset));
        }

        private void readFragments(HttpObject msg) {
            if (msg instanceof HttpContent) {
                ByteBuf bb = ((HttpContent) msg).content();
                byte[] heap = new byte[bb.readableBytes()];
                bb.readBytes(heap);
                fragmentPublisher.feed(heap);
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) { responseEnded(); }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (resultsAdapter != null) resultsAdapter.publisher.complete(cause);
            if (mediaTypeTask != null && !mediaTypeTask.isDone())
                mediaTypeTask.complete(null);
            if (fragmentPublisher != null) fragmentPublisher.complete(cause);
            failure = cause;
            ctx.close();
        }
    }
}
