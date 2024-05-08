package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties;
import com.github.alexishuf.fastersparql.client.netty.util.NettyChannelDebugger;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.util.ReferenceCounted;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static io.netty.handler.codec.http.HttpHeaderNames.*;

public final class NettyHttpClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(NettyHttpClient.class);
    private final static boolean DEBUG = FSNettyProperties.debugClientChannel();

    public static final String HANDLER_NAME = "handler";

    public static final AbortRequest ABORT_REQUEST = new AbortRequest();
    public static final class AbortRequest extends RuntimeException {
        private AbortRequest() { super("abort NettyHttpClient.request()");}
    }

    private final EventLoopGroupHolder groupHolder;
    private final ActiveChannelSet activeChannels;
    private final String host, baseUri;
    private final @Nullable SimpleChannelPool pool;
    private final Bootstrap bootstrap;
    private final String connectionHeaderValue;
    private final ChannelRecycler recycler;
    public final boolean info = FSNettyProperties.channelInfo();
    private boolean closed = false;

    public NettyHttpClient(EventLoopGroupHolder groupHolder, String baseUri,
                           Supplier<? extends NettyHttpHandler> handlerFactory,
                           boolean pool, boolean poolFIFO, @Nullable SslContext sslContext) {
        this.activeChannels = new ActiveChannelSet(baseUri);
        this.baseUri = baseUri;
        String host;
        int port = 80;
        try {
            var uri = new URI(baseUri);
            host = uri.getHost();
            port = Math.max(port, uri.getPort());
        } catch (URISyntaxException e) {
            host = baseUri;
        }
        this.host = host;
        this.bootstrap = groupHolder.acquireBootstrap(host, port);
        try {
            if (pool) {
                this.pool = new SimpleChannelPool(bootstrap, new AbstractChannelPoolHandler() {
                    @Override public void channelAcquired(Channel ch) {
                        activeChannels.setActive(ch);
                    }

                    @Override public void channelReleased(Channel ch) {
                        activeChannels.setInactive(ch);
                    }

                    @Override public void channelCreated(Channel ch) {
                        activeChannels.add(ch);
                        setupPipeline(ch, sslContext, handlerFactory);
                    }
                }, ChannelHealthChecker.ACTIVE, true, !poolFIFO);
                this.recycler = new PoolRecycler(this.pool);
                this.connectionHeaderValue = "keep-alive";
            } else {
                this.pool = null;
                this.recycler = ChannelRecycler.CLOSE;
                var initializer = new ChannelInitializer<>() {
                    @Override protected void initChannel(Channel ch) {
                        activeChannels.add(ch).setActive(ch);
                        setupPipeline(ch, sslContext, handlerFactory);
                        ch.closeFuture().addListener(ignored -> activeChannels.setInactive(ch));
                    }
                };
                this.bootstrap.handler(initializer);
                this.connectionHeaderValue = "close";
            }
            this.groupHolder = groupHolder;
        } catch (Throwable t) {
            groupHolder.release();
            throw t;
        }
    }

    private record PoolRecycler(SimpleChannelPool pool) implements ChannelRecycler {
        @Override public void recycle(Channel channel) {
            journal("recycle ", channel, "to pool");
            pool.release(channel);
        }
    }

    private static void
    setupPipeline(Channel ch, @Nullable SslContext sslContext,
                  Supplier<? extends NettyHttpHandler> hFactory) {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslContext != null)
            pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()));
        pipeline.addLast("http", new HttpClientCodec());
        pipeline.addLast("decompress", new HttpContentDecompressor());
        if (DEBUG)
            pipeline.addLast("debugger", new NettyChannelDebugger("NettyHttpClient"));
//        pipeline.addLast("log", new LoggingHandler(NettyHttpClient.class,
//                                                         LogLevel.INFO, ByteBufFormat.SIMPLE));
        NettyHttpHandler handler = hFactory.get();
        pipeline.addLast(HANDLER_NAME, handler);
    }

    /**
     * Get a executor that allows scheduling arbitrary task in a thread of the
     * {@link EventLoopGroup} that is used to process events for {@link Channel}s established by
     * {@link #connect(ConnectionHandler)}.
     *
     * @return An {@link Executor}
     */
    public @NonNull EventLoopGroup executor() { return bootstrap.config().group(); }

    public static FullHttpRequest makeRequest(HttpMethod method, String pathAndParams,
                                              @Nullable String accept,
                                              String contentType,
                                              ByteBuf body) {
        var req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, pathAndParams, body);
        HttpHeaders headers = req.headers();
        headers.set(CONTENT_TYPE, contentType);
        headers.set(CONTENT_LENGTH, body.readableBytes());
        if (accept != null)
            headers.set(ACCEPT, accept);
        return req;
    }

    public static FullHttpRequest makeGet(String pathAndParams, @Nullable String accept) {
        var req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, pathAndParams);
        if (accept != null)
            req.headers().set(ACCEPT, accept);
        return req;
    }

    public interface ConnectionHandler<T extends NettyHttpHandler>
            extends GenericFutureListener<Future<?>> {
        /**
         * Called when an active channel to the remote server is acquired.
         *
         * <p>If an implementation throws {@link #ABORT_REQUEST}, the channel will be closed
         * or returned to the pool, the request will not be sent and this handler will no longer
         * be invoked.</p>
         *
         * @param ch the channel
         * @param handler the {@link NettyHttpHandler} for {@code ch}
         */
        void onConnected(Channel ch, T handler);

        /**
         * Notifies that the request has been sent.
         *
         * @param handler the {@link NettyHttpHandler} of the channel
         * @param cookie a cookie for use with {@link NettyHttpHandler#cancel(int)}
         */
        void onStarted(T handler, int cookie);

        /**
         * Called if could not acquire a channel to the remote server.
         * @param cause an explanation
         */
        void onConnectionError(Throwable cause);

        /**
         * Called after a non-throwing {@link #onConnected(Channel, NettyHttpHandler)}.
         * The returned {@link HttpRequest} will be sent to the remote server after headers
         * are set as per {@link NettyHttpClient} configuration.
         *
         * @return a {@link HttpRequest} to send.
         */
        HttpRequest httpRequest();

        /**
         * The {@link NettyHttpClient} from which this handler will have
         * {@link #operationComplete(Future)} called.
         */
        NettyHttpClient httpClient();

        @Override default void operationComplete(Future<?> future) throws Exception {
            NettyHttpClient client = httpClient();
            HttpRequest request = null;
            boolean sent = false;
            int cookie;
            T handler = null;
            try {
                var ch = future instanceof ChannelFuture f ? f.channel() : (Channel)future.get();
                journal("connected ch=", ch, "connHandler=", this);
                //noinspection unchecked
                handler = (T) ch.pipeline().get(HANDLER_NAME);
                if (handler == null) {
                    var msg = client.baseUri+" closed connection before the local Channel initialized";
                    throw new FSException(msg);
                }
                onConnected(ch, handler);
                request = httpRequest();
                var headers = request.headers();
                headers.set(CONNECTION, client.connectionHeaderValue);
                headers.set(HOST, client.host);
                if (client.info)
                    headers.set("x-fastersparql-info", ch.id().asShortText());
                if (request instanceof HttpContent hc && !headers.contains(CONTENT_LENGTH))
                    headers.set(CONTENT_LENGTH, hc.content().readableBytes());
                cookie = handler.start(client.recycler, request);
                sent = true;
                onStarted(handler, cookie);
            } catch (Throwable t) {
                // caller expects a request to be release()ed
                if (!sent && request instanceof ReferenceCounted r)
                    r.release();
                if (t == ABORT_REQUEST) {
                    journal("abort request connHandler=", this);
                    try {
                        if (handler != null) handler.cancelBeforeStart();
                    } catch (Throwable t2) {
                        log.error("{} while handling ABORT_REQUEST on {}",
                                  t2.getClass().getSimpleName(), this, t2);
                        onConnectionError(t2);
                    }
                } else {
                    var cause = t instanceof ExecutionException ee ? ee.getCause() : t;
                    journal(cause, "connHandler=", this);
                    onConnectionError(cause);
                }
            }
        }
    }

    public <T extends NettyHttpHandler> void
    connect(ConnectionHandler<T> handler) {
        if (closed) throw new IllegalStateException("already close()ed");
        Future<?> future = pool == null ? bootstrap.connect() : pool.acquire();
        //noinspection rawtypes,unchecked
        future.addListener((GenericFutureListener)handler);
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        activeChannels.close();
        groupHolder.release();
        if (pool != null)
            pool.close();
    }
}
