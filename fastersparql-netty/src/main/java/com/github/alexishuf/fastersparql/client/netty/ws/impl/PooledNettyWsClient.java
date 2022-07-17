package com.github.alexishuf.fastersparql.client.netty.ws.impl;

import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.netty.http.ActiveChannelSet;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsRecycler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.net.InetSocketAddress.createUnresolved;

@Slf4j
public class PooledNettyWsClient implements NettyWsClient {
    private final EventLoopGroupHolder elgHolder;
    private final SimpleChannelPool pool;
    private final ActiveChannelSet activeChannels;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public PooledNettyWsClient(EventLoopGroupHolder elgHolder, URI uri,
                               HttpHeaders headers, boolean poolFIFO, @Nullable SslContext sslCtx) {
        this.activeChannels = new ActiveChannelSet(uri.toString());
        this.elgHolder = elgHolder;
        InetSocketAddress address = createUnresolved(uri.getHost(), Protocol.port(uri));
        Bootstrap bootstrap = elgHolder.acquireBootstrap(address);
        try {
            boolean lifo = !poolFIFO;
            WsRecycler recycler = this::recycle;
            WsChannelInitializer init = new WsChannelInitializer(sslCtx, uri, headers, recycler);
            pool = new SimpleChannelPool(bootstrap, new AbstractChannelPoolHandler() {
                @Override public void channelAcquired(Channel ch) {
                    activeChannels.setActive(ch);
                }
                @Override public void channelReleased(Channel ch) {
                    activeChannels.setInactive(ch);
                }
                @Override public void channelCreated(Channel ch) {
                    activeChannels.add(ch);
                    init.initChannel(ch);
                }
            }, ChannelHealthChecker.ACTIVE, true, lifo);
        } catch (Throwable e) {
            elgHolder.release();
            throw e;
        }
    }

    private void recycle(Channel channel) {
        pool.release(channel);
    }

    @Override public void open(WsClientHandler handler) {
        UnpooledNettyWsClient.retryingOpen(handler, pool::acquire);
    }

    @Override public void close() {
        if (closed.compareAndSet(false, true)) {
            activeChannels.close();
            elgHolder.release();
        }
    }
}
