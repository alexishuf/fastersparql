package com.github.alexishuf.fastersparql.client.netty.ws.impl;

import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientNettyHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsRecycler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.net.InetSocketAddress.createUnresolved;

@Slf4j
public class UnpooledNettyWsClient implements NettyWsClient {
    private final EventLoopGroupHolder elgHolder;
    private final Bootstrap bootstrap;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public UnpooledNettyWsClient(EventLoopGroupHolder elgHolder, URI uri,
                                 HttpHeaders headers, @Nullable SslContext sslContext) {
        this.elgHolder = elgHolder;
        InetSocketAddress address = createUnresolved(uri.getHost(), uri.getPort());
        EventLoopGroup group = elgHolder.acquire();
        try {
            this.bootstrap = new Bootstrap().remoteAddress(address).group(group)
                    .channel(elgHolder.transport().channelClass())
                    .handler(new WsChannelInitializer(sslContext, uri, headers, WsRecycler.CLOSE));
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public void open(WsClientHandler handler) {
        ChannelFuture f = bootstrap.connect();
        f.addListener(ignored -> {
            if (f.isSuccess())
                ((WsClientNettyHandler) f.channel().pipeline().get("ws")).delegate(handler);
            else
                handler.onError(f.cause());
        });
    }

    @Override public void close() {
        if (closed.compareAndSet(false, true))
            elgHolder.release();
    }
}
