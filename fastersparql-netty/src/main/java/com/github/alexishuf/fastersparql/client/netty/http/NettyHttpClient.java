package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRopeUtils;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.netty.handler.codec.http.HttpHeaderNames.*;

public final class NettyHttpClient implements AutoCloseable {
    public static final String HANDLER_NAME = "handler";

    private final EventLoopGroupHolder groupHolder;
    private final ActiveChannelSet activeChannels;
    private final String host, baseUri;
    private final @Nullable SimpleChannelPool pool;
    private final Bootstrap bootstrap;
    private final String connectionHeaderValue;
    private final ChannelRecycler recycler;

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
                this.recycler = this.pool::release;
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

    private static NettyHttpHandler
    setupPipeline(Channel ch, @Nullable SslContext sslContext,
                  Supplier<? extends NettyHttpHandler> hFactory) {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslContext != null)
            pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()));
        pipeline.addLast("http", new HttpClientCodec());
        pipeline.addLast("decompress", new HttpContentDecompressor());

        NettyHttpHandler handler = hFactory.get();
        pipeline.addLast(HANDLER_NAME, handler);
        return handler;
    }


    public static HttpRequest makeRequest(HttpMethod method, String pathAndParams,
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

    public static HttpRequest makeGet(String pathAndParams, @Nullable String accept) {
        var req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, pathAndParams);
        if (accept != null)
            req.headers().set(ACCEPT, accept);
        return req;
    }

    public <T extends NettyHttpHandler> void
    request(HttpRequest request, BiConsumer<Channel, T> onConnected, Consumer<Throwable> onError) {
        (pool == null ? bootstrap.connect() : pool.acquire()).addListener(f -> {
            try {
                Channel ch = f instanceof ChannelFuture cf ? cf.channel() : (Channel) f.get();
                var headers = request.headers();
                headers.set(CONNECTION, connectionHeaderValue);
                headers.set(HOST, host);
                if (request instanceof HttpContent hc && !headers.contains(CONTENT_LENGTH))
                    headers.set(CONTENT_LENGTH, hc.content().readableBytes());
                //noinspection unchecked
                T handler = (T) ch.pipeline().get(HANDLER_NAME);
                if (handler == null) {
                    var msg = baseUri+" closed connection before the local Channel initialized";
                    throw new FSException(msg);
                }
                onConnected.accept(ch, handler);
                ch.writeAndFlush(request);
            } catch (Throwable t) {
                onError.accept(t instanceof ExecutionException ? t.getCause() : t);
            }
        });
    }

    @Override public void close() {
        activeChannels.close();
        groupHolder.release();
        if (pool != null)
            pool.close();
    }
}
