package com.github.alexishuf.fastersparql.client.netty.ws.impl;

import com.github.alexishuf.fastersparql.client.netty.http.ActiveChannelSet;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientNettyHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsRecycler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
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
    private final ActiveChannelSet activeChannels = new ActiveChannelSet();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public PooledNettyWsClient(EventLoopGroupHolder elgHolder, URI uri,
                               HttpHeaders headers, boolean poolFIFO, @Nullable SslContext sslCtx) {
        this.elgHolder = elgHolder;
        InetSocketAddress address = createUnresolved(uri.getHost(), uri.getPort());
        EventLoopGroup group = elgHolder.acquire();
        try {
            Bootstrap bootstrap = new Bootstrap().remoteAddress(address).group(group)
                    .channel(elgHolder.transport().channelClass());
            boolean lifo = !poolFIFO;
            WsRecycler recycler = ch -> new WsRecycler() {
                @Override public void recycle(Channel channel) {
                    PooledNettyWsClient.this.pool.release(channel);
                }
            };
            WsChannelInitializer init = new WsChannelInitializer(sslCtx, uri, headers, recycler);
            pool = new SimpleChannelPool(bootstrap, new AbstractChannelPoolHandler() {
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

    @Override public void open(WsClientHandler handler) {
        pool.acquire().addListener(f -> {
            Channel ch;
            try {
                ch = (Channel)f.get();
            } catch (Throwable e) {
                handler.onError(e);
                return;
            }
            ((WsClientNettyHandler)ch.pipeline().get("ws")).delegate(handler);
        });
    }

    @Override public void close() {
        if (closed.compareAndSet(false, true)) {
            activeChannels.close();
            elgHolder.release();
        }
    }
}
