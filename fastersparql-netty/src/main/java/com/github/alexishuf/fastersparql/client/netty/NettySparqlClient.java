package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.model.*;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;
import com.github.alexishuf.fastersparql.client.model.row.impl.StringArrayOperations;
import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.results.*;
import com.github.alexishuf.fastersparql.client.parser.row.RowParser;
import com.github.alexishuf.fastersparql.client.util.MediaType;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.client.util.async.AsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.SafeAsyncTask;
import com.github.alexishuf.fastersparql.client.util.async.SafeCompletableAsyncTask;
import com.github.alexishuf.fastersparql.client.util.bind.BindPublisher;
import com.github.alexishuf.fastersparql.client.util.bind.Binder;
import com.github.alexishuf.fastersparql.client.util.bind.SparqlClientBinder;
import com.github.alexishuf.fastersparql.client.util.reactive.CallbackPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.EmptyPublisher;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.lang.System.identityHashCode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;


public class NettySparqlClient<R, F> implements SparqlClient<R, F> {
    private static final AtomicInteger nextBindPublisherId = new AtomicInteger(1);
    private static final Logger log = LoggerFactory.getLogger(NettySparqlClient.class);
    private static final Set<SparqlMethod> SUPPORTED_METHODS = unmodifiableSet(new HashSet<>(asList(
            SparqlMethod.GET, SparqlMethod.FORM, SparqlMethod.POST
    )));

    private final AtomicLong nextHandler = new AtomicLong(1);
    private final SparqlEndpoint endpoint;
    private final AsyncTask<NettyHttpClient<Handler>> netty;
    private final RowParser<R> rowParser;
    private final FragmentParser<F> fragParser;
    private final Supplier<Handler> handlerFactory =
            () -> new Handler(this +"-"+nextHandler.getAndIncrement());


    public NettySparqlClient(SparqlEndpoint endpoint, RowParser<R> rowParser,
                             FragmentParser<F> fragmentParser) {
        if (endpoint == null) throw new NullPointerException("endpoint is null");
        if (rowParser == null) throw new NullPointerException("rowParser is null");
        if (fragmentParser == null) throw new NullPointerException("fragmentParser is null");
        this.endpoint = withSupported(endpoint, ResultsParserRegistry.get(), SUPPORTED_METHODS);
        this.netty = endpoint.resolvedHost().thenApplyThrowing(a ->
                new NettyClientBuilder().buildHTTP(endpoint.protocol(), a, handlerFactory));
        this.rowParser = rowParser;
        this.fragParser = fragmentParser;
    }

    @Override public Class<R> rowClass() {
        //noinspection unchecked
        return (Class<R>) rowParser.rowClass();
    }

    @Override public Class<F> fragmentClass() {
        //noinspection unchecked
        return (Class<F>) fragParser.fragmentClass();
    }

    @Override public SparqlEndpoint endpoint() {
        return endpoint;
    }

    @Override public String toString() {
        return String.format("NettySparqlClient[%s]@%x", endpoint.uri(), identityHashCode(this));
    }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration,
                            @Nullable Results<R> bindings, @Nullable BindType bindType) {
        if (bindings == null)
            return query(sparql, configuration);
        else if (bindType == null)
            throw new NullPointerException("bindings != null, but bindType is null!");
        RowOperations rowOps = RowOperationsRegistry.get().forClass(rowClass());
        Binder<R> binder = new SparqlClientBinder<>(rowOps, bindings.vars(), this, sparql,
                                                     configuration, bindType);
        String name = this.toString()+bindType+"-"+nextBindPublisherId.getAndIncrement();
        BindPublisher<R> publisher = new BindPublisher<>(bindings.publisher(), 1,
                                                         binder, name, null);
        return new Results<>(binder.resultVars(), rowClass(), publisher);
    }

    @Override
    public Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        List<String> vars = SparqlUtils.publicVars(sparql);
        Throwable cause;
        try {
            PublisherAdapter<String[]> publisher = new PublisherAdapter<>();
            publisher.requester = () -> {
                try {
                    SparqlConfiguration eff = effectiveConfig(endpoint, configuration, sparql.length());
                    SparqlMethod method = eff.methods().get(0);
                    HttpMethod nettyMethod = method2netty(method);
                    String accept = resultsAcceptString(eff.resultsAccepts());
                    netty.get().request(nettyMethod, firstLine(endpoint, eff, sparql),
                            nettyMethod == HttpMethod.GET ? null : a -> generateBody(a, eff, sparql),
                            new QueryHandlerSetup(vars, accept, method, publisher));
                } catch (Throwable t) {
                    publisher.complete(t);
                }
            };
            Results<String[]> raw = new Results<>(vars, String[].class, publisher);
            FSPublisher<R> parsedPub = rowParser.parseStringsArray(raw);
            if (parsedPub == raw.publisher()) //noinspection unchecked
                return (Results<R>) raw;
            return new Results<>(vars, rowParser.rowClass(), parsedPub);
        } catch (Throwable t) {
            cause = t;
        }
        return Results.error(vars, rowParser.rowClass(), cause);
    }

    @Override
    public Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        Throwable cause;
        try {
            PublisherAdapter<byte[]> publisher = new PublisherAdapter<>();
            SafeCompletableAsyncTask<MediaType> mtTask = new SafeCompletableAsyncTask<>();
            publisher.requester = () -> {
                try {
                    SparqlConfiguration eff = effectiveConfig(endpoint, configuration, sparql.length());
                    SparqlMethod method = eff.methods().get(0);
                    HttpMethod nettyMethod = method2netty(method);
                    MediaType errorMT = eff.rdfAccepts().get(0);
                    String accept = rdfAcceptString(eff.rdfAccepts());
                    netty.get().request(nettyMethod, firstLine(endpoint, eff, sparql),
                            nettyMethod == HttpMethod.GET ? null : a -> generateBody(a, eff, sparql),
                            new GraphHandlerSetup(mtTask, errorMT, accept, method, publisher));
                } catch (Throwable t) {
                    mtTask.complete(new MediaType("text", "plain"));
                    publisher.complete(t);
                }
            };
            Graph<byte[]> raw = new Graph<>(mtTask, byte[].class, publisher);
            FSPublisher<F> parsedPub = fragParser.parseBytes(raw);
            if (parsedPub == raw.publisher()) //noinspection unchecked
                return (Graph<F>) raw;
            return new Graph<>(mtTask, fragParser.fragmentClass(), parsedPub);
        } catch (Throwable t) {
            cause = t;
        }
        SafeAsyncTask<MediaType> nullMT = Async.wrap((MediaType) null);
        FSPublisher<F> empty = FSPublisher.bindToAny(new EmptyPublisher<>(cause));
        return new Graph<>(nullMT, fragParser.fragmentClass(), empty);
    }

    @Override public void close() {
        boolean cancel = !netty.isDone();
        if (cancel)
            netty.cancel(true);
        NettyHttpClient<Handler> client = null;
        try {
            client = netty.orElse(null);
        } catch (CancellationException ignored) {
        } catch (ExecutionException e) {
            if (cancel) {
                log.debug("{}.close(): NettyHttpClient construction failed after cancel: ",
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

    @RequiredArgsConstructor
    private abstract static class HandlerSetupBase<T> implements NettyHttpClient.Setup<Handler> {
        protected final String accept;
        protected final SparqlMethod method;
        protected final PublisherAdapter<T> publisher;

        @Override public void setup(Channel ch, HttpRequest request, Handler handler) {
            request.headers().set(HttpHeaderNames.ACCEPT, accept);
            if (method.hasRequestBody())
                request.headers().set(CONTENT_TYPE, method.contentType());
            ch.eventLoop().execute(() -> ch.config().setAutoRead(true));
            setupHandler(ch, handler);
        }

        protected abstract void setupHandler(Channel ch, Handler handler);

        @Override public void connectionError(Throwable cause) {
            log.debug("connectionError, completing the publisher", cause);
            publisher.complete(cause);
        }

        @Override public void requestError(Throwable cause) {
            log.error("Unexpected error when building the request on a connected channel", cause);
            publisher.complete(cause);
        }
    }


    private static final class QueryHandlerSetup extends HandlerSetupBase<String[]> {
        private final List<String> vars;

        public QueryHandlerSetup(List<String> vars, String accept, SparqlMethod method,
                                 PublisherAdapter<String[]> publisher) {
            super(accept, method, publisher);
            this.vars = vars;
        }

        @Override protected void setupHandler(Channel ch, Handler handler) {
            handler.setupResults(ch, vars, publisher);
        }
    }

    private static final class GraphHandlerSetup extends HandlerSetupBase<byte[]> {
        private final SafeCompletableAsyncTask<MediaType> mtTask;
        private final MediaType mtOnEarlyError;

        public GraphHandlerSetup(SafeCompletableAsyncTask<MediaType>  mtTask,
                                 MediaType mtOnEarlyError, String accept,
                                 SparqlMethod method, PublisherAdapter<byte[]> publisher) {
            super(accept, method, publisher);
            this.mtTask = mtTask;
            this.mtOnEarlyError = mtOnEarlyError;
        }

        @Override protected void setupHandler(Channel ch, Handler handler) {
            handler.setupGraph(ch, mtTask, publisher);
        }

        @Override public void connectionError(Throwable cause) {
            mtTask.complete(mtOnEarlyError);
            super.connectionError(cause);
        }

        @Override public void requestError(Throwable cause) {
            mtTask.complete(mtOnEarlyError);
            super.requestError(cause);
        }
    }

    /**
     * The {@link Publisher} exposed by {@link NettySparqlClient} query methods
     * (when no row/fragment parser is used)
     */
    private static class PublisherAdapter<T> extends CallbackPublisher<T> {
        private static final  Logger log = LoggerFactory.getLogger(PublisherAdapter.class);
        private static final AtomicInteger nextId = new AtomicInteger(1);

        private Throwing.@MonotonicNonNull Runnable requester;
        private boolean pendingAutoRead, pendingCancel;
        private @MonotonicNonNull Handler handler;
        private int cycle = -1;
        public PublisherAdapter() {
            super("NettySparqlClient.PublisherAdapter-"+nextId.getAndIncrement());
        }
        public synchronized void handler(Handler handler, int cycle) {
            log.trace("{}.handler({})", this, handler);
            this.handler = handler;
            this.cycle = cycle;
            if (pendingCancel)
                this.handler.abort(cycle);
            else if (pendingAutoRead)
                this.handler.autoRead(cycle, true);
        }

        @Override public void subscribe(Subscriber<? super T> s) {
            Throwable cause = null;
            if (!isSubscribed()) {
                try {
                    requester.run();
                } catch (ExecutionException e) {
                    cause = e.getCause() == null ? e : e.getCause();
                } catch (Throwable t) {
                    cause = t;
                }
            }
            super.subscribe(s);
            if (cause != null)
                s.onError(cause);
        }

        @Override protected synchronized void onRequest(long n) {
            log.trace("{}.onRequest({}), handler={}", this, n, handler);
            if (handler != null) handler.autoRead(cycle, true);
            else                 pendingAutoRead = true;
        }
        @Override protected synchronized void onBackpressure() {
            if (handler != null) handler.autoRead(cycle, false);
        }
        @Override protected synchronized void onCancel() {
            if (handler != null) handler.abort(cycle);
            else                 pendingCancel = true;
        }
    }
    /**
     * Listens as a {@link ResultsParserConsumer} and feeds a {@link PublisherAdapter}.
     */
    @RequiredArgsConstructor
    private static class ResultsParserAdapter implements ResultsParserConsumer {
        private static final StringArrayOperations ARRAY_OPS = StringArrayOperations.get();
        private final PublisherAdapter<String[]> publisher;
        private Merger<String[]> projector;

        public ResultsParserAdapter(List<String> expectedVars,
                                    PublisherAdapter<String[]> publisher) {
            this.projector = Merger.identity(ARRAY_OPS, expectedVars);
            this.publisher = publisher;
        }
        @Override public void vars(List<String> vars) {
            projector = Merger.forProjection(ARRAY_OPS, projector.outVars(), vars);
        }
        @Override public void row(@Nullable String[] row) {
            publisher.feed(projector.merge(row, null));
        }
        @Override public void end() { publisher.complete(null); }
        @Override public void onError(String message) {
            publisher.complete(new InvalidSparqlResultsException(message));
        }
    }


    private static class Handler extends SimpleChannelInboundHandler<HttpObject>
            implements ReusableHttpClientInboundHandler {
        private static final Logger log = LoggerFactory.getLogger(Handler.class);
        private final String name;
        private int cycle = 0;
        private Runnable onResponseEnd;
        private @MonotonicNonNull Channel channel;
        private Throwable failure;
        private ResultsParserAdapter resultsAdapter;
        private PublisherAdapter<byte[]> fragmentPublisher;
        private SafeCompletableAsyncTask<MediaType> mediaTypeTask;
        private ResultsParser resultsParser;
        private MediaType mediaType;
        private Charset charset = UTF_8;

        public Handler(String name) { this.name = name; }

        @Override public String toString() {
            if (resultsAdapter != null) return name+"["+resultsAdapter.publisher+"]";
            return name;
        }

        @Override public void onResponseEnd(Runnable runnable) { this.onResponseEnd = runnable; }

        protected void responseEnded() {
            // as the Channel may be reused, from this point onwards refuse abort()/setAutoRead()
            // from the PublisherAdapter
            ++cycle;
            assert channel != null : "responseEnded() channelRegistered()";
            assert channel.eventLoop().inEventLoop() : "responseEnded() not run in eventLoop()";
            if (resultsParser != null) resultsParser.end();
            else if (resultsAdapter != null) resultsAdapter.end();
            if (fragmentPublisher != null) fragmentPublisher.complete(null);
            channel.config().setAutoRead(true); //return to pool with autoRead enabled
            if (onResponseEnd != null) onResponseEnd.run();
        }

        private void reset(Channel channel) {
            assert channel != null : "reset() before channelRegistered()";
            assert channel.eventLoop().inEventLoop(): "reset() not running in event loop";
            this.failure = null;
            this.mediaType = null;
            this.charset = null;
            this.fragmentPublisher = null;
            this.mediaTypeTask = null;
            this.resultsParser = null;
            this.resultsAdapter = null;
            assert this.channel == null || this.channel == channel;
            this.channel = channel;
        }

        public void setupResults(Channel channel, List<String> outVars,
                                              PublisherAdapter<String[]> rowPublisher) {
            reset(channel);
            this.resultsAdapter = new ResultsParserAdapter(outVars, rowPublisher);
            rowPublisher.handler(this, cycle);
        }

        public void setupGraph(Channel channel,
                                            SafeCompletableAsyncTask<MediaType> mediaTypeTask,
                                            PublisherAdapter<byte[]> fragmentPublisher) {
            reset(channel);
            this.mediaTypeTask = mediaTypeTask;
            (this.fragmentPublisher = fragmentPublisher).handler(this, cycle);
        }

        public void autoRead(int cycle, boolean value) {
            assert channel != null;
            EventLoop el = channel.eventLoop();
            if (el.inEventLoop())
                doAutoRead("", cycle, value);
            else
                el.execute(() -> doAutoRead(" runnable", cycle, value));
        }

        private void doAutoRead(String suffix, int cycle, boolean value) {
            assert this.channel.eventLoop().inEventLoop();
            if (this.cycle == cycle) {
                log.trace("{}.autoRead({}){}: setting", this, value, suffix);
                channel.config().setAutoRead(value);
            } else {
                log.trace("{}.autoRead({}){}: stale cycle", this, value, suffix);
            }
        }

        public void abort(int cycle) {
            assert channel != null;
            EventLoop el = channel.eventLoop();
            if (el.inEventLoop())
                doAbort("", cycle);
            else
                el.execute(() -> doAbort(" runnable", cycle));
        }

        private void doAbort(String suffix, int cycle) {
            assert this.channel.eventLoop().inEventLoop();
            if (this.cycle == cycle) {
                log.trace("{}.abort(){}: close()ing", this, suffix);
                channel.close();
            } else {
                log.trace("{}.abort(){}: stale cycle", this, suffix);
            }
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
            if (msg instanceof LastHttpContent) {
                log.trace("{}.channelRead0: LastHttpContent", this);
                responseEnded();
            }
        }

        private void readRows(HttpObject msg) throws NoParserException {
            if (msg instanceof HttpResponse)
                resultsParser = ResultsParserRegistry.get().createFor(mediaType, resultsAdapter);
            if (msg instanceof HttpContent) {
                String string = ((HttpContent) msg).content().toString(charset);
                log.trace("{} << {}", this, string);
                resultsParser.feed(string);
            }
        }

        private void readFragments(HttpObject msg) {
            if (msg instanceof HttpContent) {
                ByteBuf bb = ((HttpContent) msg).content();
                byte[] heap = new byte[bb.readableBytes()];
                bb.readBytes(heap);
                fragmentPublisher.feed(heap);
            }
        }

        @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            channel = ctx.channel();
            super.channelRegistered(ctx);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            log.trace("{}.channelInactive", this);
            responseEnded();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("{}.exceptionCaught({})", this, cause);
            if (resultsAdapter != null) resultsAdapter.publisher.complete(cause);
            if (mediaTypeTask != null && !mediaTypeTask.isDone())
                mediaTypeTask.complete(null);
            if (fragmentPublisher != null) fragmentPublisher.complete(cause);
            failure = cause;
            ctx.close();
        }
    }
}
