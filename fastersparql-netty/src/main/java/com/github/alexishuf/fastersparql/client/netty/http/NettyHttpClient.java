package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.util.*;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static io.netty.handler.codec.http.HttpHeaderNames.*;

public final class NettyHttpClient implements AutoCloseable {
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
                        setupPipeline(ch, sslContext, handlerFactory).recycler(recycler);
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
            ThreadJournal.journal("recycle ", channel, "to pool");
            pool.release(channel);
        }
    }

    private static NettyHttpHandler
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
        return handler;
    }


    public static FullHttpRequest makeRequest(HttpMethod method, String pathAndParams,
                                              @Nullable String accept,
                                              String contentType,
                                              CharSequence body, Charset charset) {
        ByteBuf bb =  NettyRopeUtils.wrap(body, charset);
        var req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, pathAndParams, bb);
        HttpHeaders headers = req.headers();
        headers.set(CONTENT_TYPE, contentType);
        headers.set(CONTENT_LENGTH, bb.readableBytes());
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

    public <T extends NettyHttpHandler> void handleChannel(Future<?> f, ConnectionHandler<T> h) {
        HttpRequest request = h.httpRequest();
        boolean sent = false;
        Channel ch = null;
        try {
            ch = f instanceof ChannelFuture cf ? cf.channel() : (Channel) f.get();
            var headers = request.headers();
            headers.set(CONNECTION, connectionHeaderValue);
            headers.set(HOST, host);
            if (info)
                headers.set("x-fastersparql-info", ch.id().asShortText());
            if (request instanceof HttpContent hc && !headers.contains(CONTENT_LENGTH))
                headers.set(CONTENT_LENGTH, hc.content().readableBytes());
            //noinspection unchecked
            T handler = (T) ch.pipeline().get(HANDLER_NAME);
            if (handler == null) {
                var msg = baseUri+" closed connection before the local Channel initialized";
                throw new FSException(msg);
            }
            h.onConnected(ch, handler);
            sent = true;
            ch.writeAndFlush(request);
        } catch (Throwable t) {
            // caller expects a request to be release()ed
            if (!sent && request instanceof ReferenceCounted r)
                r.release();
            if (t == ABORT_REQUEST) {
                if (ch != null) recycler.recycle(ch);
            } else {
                h.onConnectionError(t instanceof ExecutionException ? t.getCause() : t);
            }
        }
    }

    public interface ConnectionHandler<T extends NettyHttpHandler>
            extends GenericFutureListener<Future<?>> {
        void onConnected(Channel ch, T handler);
        void onConnectionError(Throwable cause);
        HttpRequest httpRequest();
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
