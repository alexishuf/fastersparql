package com.github.alexishuf.fastersparql.client.netty.ws;

import com.github.alexishuf.fastersparql.client.netty.http.ActiveChannelSet;
import com.github.alexishuf.fastersparql.client.netty.util.ChannelRecycler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;
import java.util.concurrent.ExecutionException;

public class NettyWsClient implements AutoCloseable {
    private final EventLoopGroupHolder elgHolder;
    private final Bootstrap bootstrap;
    private final @Nullable SimpleChannelPool pool;
    private final ActiveChannelSet activeChs;
    private boolean closed;

    public NettyWsClient(EventLoopGroupHolder elgHolder, URI uri,
                         HttpHeaders headers, boolean pool,
                         boolean poolFIFO, @Nullable SslContext sslContext) {
        this.elgHolder = elgHolder;
        this.activeChs = new ActiveChannelSet(uri.toString());
        int maxHttp = FSNettyProperties.wsMaxHttpResponse();
        ChannelRecycler recycler = pool ? this::recycle : ChannelRecycler.CLOSE;
        var initializer = new ChannelInitializer<>() {
            @Override protected void initChannel(Channel ch) {
                ChannelPipeline pipe = ch.pipeline();
                if (sslContext != null)
                    pipe.addLast("ssl", sslContext.newHandler(ch.alloc()));
                pipe.addLast("http", new HttpClientCodec());
                pipe.addLast("aggregator", new HttpObjectAggregator(maxHttp));
                pipe.addLast("comp", WebSocketClientCompressionHandler.INSTANCE);
                pipe.addLast("ws", new NettWsClientPipelineHandler(uri, headers, recycler));
                activeChs.add(ch);
            }
        };
        int port = uri.getPort() > 0 ? uri.getPort() : (uri.getScheme().endsWith("s") ? 443 : 80);
        this.bootstrap = elgHolder.acquireBootstrap(uri.getHost(), port);
        try {
            if (pool) {
                this.pool = new SimpleChannelPool(bootstrap, new AbstractChannelPoolHandler() {
                    @Override public void channelAcquired(Channel c) { activeChs.setActive(c); }
                    @Override public void channelReleased(Channel c) { activeChs.setInactive(c); }
                    @Override public void channelCreated(Channel c)  { initializer.initChannel(c); }
                }, ChannelHealthChecker.ACTIVE, true, !poolFIFO);
            } else {
                this.pool = null;
                this.bootstrap.handler(initializer);
            }
        } catch (Throwable t) {
            elgHolder.release();
            throw t;
        }
    }

    private void recycle(Channel ch) {
        if (pool != null) pool.release(ch);
    }

    /**
     * Open (or reuse an idle) WebSocket session and handle events with {@code handle}.
     *
     * <p>Eventually one of the following methods will be called:</p>
     * <ul>
     *     <li>{@link NettyWsClientHandler#attach(ChannelHandlerContext, ChannelRecycler)}
     *         once a WebSocket session has been established</li>
     *     <li>{@link NettyWsClientHandler#detach(Throwable)} with the reason for not establishing
     *         a WebSocket session (e.g., could not connect, WebSocket handshake failed, etc.).
     *         There will be no subsequent {@link NettyWsClientHandler#attach(ChannelHandlerContext, ChannelRecycler) call</li>
     * </ul>
     *
     * <p>Note that after {@code handler.attach(ctx, recycler)}, {@code onError} will still be called
     * if an error occurs after the session was established.</p>
     *
     * @param handler listener for WebSocket session events.
     */
    public void open(NettyWsClientHandler handler) {
        (pool == null ? bootstrap.connect() : pool.acquire()).addListener(f -> {
            try {
                Channel ch = f instanceof ChannelFuture cf ? cf.channel() : (Channel) f.get();
                var nettyHandler = (NettWsClientPipelineHandler) ch.pipeline().get("ws");
                if (nettyHandler == null) {
                    String msg = ch.isOpen()
                            ? "\"ws\" handler missing from pipeline in ch=" + ch
                            : "Server closed the connection prematurely for ch=" + ch;
                    handler.detach(new FSServerException(msg));
                } else {
                    nettyHandler.delegate(handler);
                }
            } catch (ExecutionException e) {
                handler.detach(e.getCause());
            } catch (Throwable t) {
                handler.detach(t);
            }
        });
    }

    @Override public void close() {
        if (!closed) {
            closed = true;
            activeChs.close();
            elgHolder.release();
        }
    }
}
