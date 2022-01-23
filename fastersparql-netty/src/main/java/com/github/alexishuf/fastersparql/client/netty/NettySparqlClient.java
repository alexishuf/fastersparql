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
import com.github.alexishuf.fastersparql.client.util.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import static com.github.alexishuf.fastersparql.client.util.SparqlClientHelpers.*;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class NettySparqlClient<R, F> implements SparqlClient<R, F> {
    private final SparqlEndpoint endpoint;
    private final AsyncTask<NettyHttpClient<Handler>> netty;
    private final RowParser<R> rowParser;
    private final FragmentParser<F> fragParser;


    public NettySparqlClient(@lombok.NonNull SparqlEndpoint endpoint,
                             @lombok.NonNull RowParser<R> rowParser,
                             @lombok.NonNull FragmentParser<F> fragmentParser) {
        this.endpoint = endpoint;
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
        Throwable cause;
        try {
            SparqlConfiguration eff = effectiveConfig(endpoint, configuration, sparql.length());
            SparqlMethod method = eff.methods().get(0);
            HttpMethod nettyMethod = method2netty(method);
            SafeCompletableAsyncTask<List<String>> vars = new SafeCompletableAsyncTask<>();
            PublisherShim<String[]> publisher = new PublisherShim<>();
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
            return new Results<>(vars, rowParser.rowClass(), rowParser.parseStringsArray(raw));
        } catch (ExecutionException e) {
            cause = e.getCause() == null ? e : e.getCause();
        } catch (Throwable t) {
            cause = t;
        }
        return new Results<>(Async.wrap(Collections.emptyList()), rowParser.rowClass(),
                             new EmptyPublisher<>(cause));
    }

    @Override
    public Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        Throwable cause;
        try {
            SparqlConfiguration eff = effectiveConfig(endpoint, configuration, sparql.length());
            SparqlMethod method = eff.methods().get(0);
            HttpMethod nettyMethod = method2netty(method);
            SafeCompletableAsyncTask<MediaType> mtTask = new SafeCompletableAsyncTask<>();
            PublisherShim<byte[]> publisher = new PublisherShim<>();
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
            return new Graph<>(mtTask, fragParser.fragmentClass(), fragParser.parseBytes(raw));
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
    @Slf4j
    private static class PublisherShim<T> implements Publisher<T> {
        private @MonotonicNonNull Handler handler;
        private long requested = 0;
        private boolean active = true;
        private Subscriber<? super T> subscriber;
        private final Queue<T> earlyRows = new ArrayDeque<>();

        @AllArgsConstructor
        private class SubscriptionHandle implements Subscription {
            private boolean active;

            private void error(Throwable t) {
                if (active && PublisherShim.this.active) {
                    subscriber.onError(t);
                    active = false;
                }
            }

            @Override public void request(long n) {
                if (n <= 0) {
                    error(new IllegalArgumentException("request("+n+"): n must be > 0"));
                } else if (active) {
                    for (; active && n > 0 && !earlyRows.isEmpty(); --n)
                        callNext(earlyRows.remove());
                    if (active && n > 0) {
                        synchronized (PublisherShim.this) {
                            requested += n;
                            if (handler != null) handler.autoRead(true);
                        }
                    }
                }
            }

            @Override public void cancel() {
                if (active) {
                    active = false;
                    Handler copy = PublisherShim.this.handler;
                    if (copy != null) copy.abort();
                }
            }
        }

        @Override public void subscribe(Subscriber<? super T> s) {
            boolean active = this.subscriber == null;
            SubscriptionHandle handle = new SubscriptionHandle(active);
            s.onSubscribe(handle);
            if (active)
                this.subscriber = s;
            else
                handle.error(new IllegalStateException(this + "can only be subscribed once"));
        }

        public void handler(Handler handler) {
            if (this.handler != null) throw new IllegalArgumentException("handler already set");
            this.handler = handler;
        }

        public void end() {
            if (active) {
                subscriber.onComplete();
                active = false;
            }
        }

        public void error(Throwable cause) {
            if (active) {
                subscriber.onError(cause);
                active = false;
            } else {
                log.info("Ignoring {} since !active", cause, cause);
            }
        }

        public void feed(T row) {
            assert handler != null : "feed() with null handler";
            synchronized (this) {
                if (requested == 0)
                    handler.autoRead(false); // propagate backpressure
                else
                    --requested;
            }
            if      (subscriber == null) earlyRows.add(row);
            else if (active            ) callNext(row);
            else                         log.debug("feed({}): dropping", row);
        }

        private void callNext(T row) {
            try {
                subscriber.onNext(row);
            } catch (Throwable t) {
                log.error("{}.onNext({}) threw {}. treating as Subscription.cancel()",
                          subscriber, row, t, t);
                active = false;
                assert handler != null;
                handler.abort();
            }
        }
    }

    /**
     * Listens as a {@link ResultsParserConsumer} and feeds a {@link PublisherShim}.
     */
    @RequiredArgsConstructor
    private static class ResultsParserAdapter implements ResultsParserConsumer {
        private final SafeCompletableAsyncTask<List<String>> varsTask;
        private final PublisherShim<String[]> publisher;

        @Override public void vars(List<String> vars) { varsTask.complete(vars);}
        @Override public void row(@Nullable String[] row) { publisher.feed(row); }
        @Override public void end() { publisher.end(); }
        @Override public void onError(String message) { error(new InvalidSparqlResultsException(message)); }

        public void error(Throwable cause) {
            if (!varsTask.isDone())
                varsTask.complete(Collections.emptyList());
            publisher.error(cause);
        }
    }

    @Slf4j
    private static class Handler extends SimpleChannelInboundHandler<HttpObject>
            implements ReusableHttpClientInboundHandler {
        private Runnable onResponseEnd;
        private Channel channel;
        private Throwable failure;
        private ResultsParserAdapter resultsAdapter;
        private PublisherShim<byte[]> fragmentPublisher;
        private SafeCompletableAsyncTask<MediaType> mediaTypeTask;
        private ResultsParser resultsParser;
        private MediaType mediaType;
        private Charset charset = UTF_8;

        @Override public void onResponseEnd(Runnable runnable) {
            this.onResponseEnd = runnable;
        }

        protected void responseEnded() {
            if (resultsParser != null) resultsParser.end();
            if (resultsAdapter != null) resultsAdapter.end();
            if (fragmentPublisher != null) fragmentPublisher.end();
            channel = null;
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

        public void setupResults(Channel channel, SafeCompletableAsyncTask<List<String>> varsTask,
                                 PublisherShim<String[]> rowPublisher) {
            reset(channel);
            this.channel = channel;
            this.resultsAdapter = new ResultsParserAdapter(varsTask, rowPublisher);
            rowPublisher.handler(this);
        }

        public void setupGraph(Channel channel, SafeCompletableAsyncTask<MediaType> mediaTypeTask,
                               PublisherShim<byte[]> fragmentPublisher) {
            reset(channel);
            this.channel = channel;
            this.mediaTypeTask = mediaTypeTask;
            (this.fragmentPublisher = fragmentPublisher).handler(this);
        }

        public void autoRead(boolean value) {
            assert channel != null : "autoRead() before setup()";
            channel.config().setAutoRead(value);
        }

        public void abort() {
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
                readFragments(ctx.alloc(), msg);
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

        private void readFragments(ByteBufAllocator alloc, HttpObject msg) {
            if (msg instanceof HttpContent) {
                ByteBuf bb = ((HttpContent) msg).content();
                int len = bb.readableBytes();
                if (bb.hasArray() && bb.arrayOffset() == 0 && bb.array().length == len) {
                    fragmentPublisher.feed(bb.array());
                } else {
                    ByteBuf heap = alloc.heapBuffer(len);
                    fragmentPublisher.feed(heap.writeBytes(bb).array());
                    heap.release();
                }
            }
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) { responseEnded(); }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (resultsAdapter != null) resultsAdapter.error(cause);
            if (mediaTypeTask != null && !mediaTypeTask.isDone())
                mediaTypeTask.complete(null);
            if (fragmentPublisher != null) fragmentPublisher.error(cause);
            failure = cause;
            ctx.close();
        }
    }
}
