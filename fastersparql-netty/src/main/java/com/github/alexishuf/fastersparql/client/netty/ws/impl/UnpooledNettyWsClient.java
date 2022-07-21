package com.github.alexishuf.fastersparql.client.netty.ws.impl;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRetryingChannelSupplier;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientNettyHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsRecycler;
import io.netty.bootstrap.Bootstrap;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import lombok.val;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.net.InetSocketAddress.createUnresolved;

public class UnpooledNettyWsClient implements NettyWsClient {
    private final EventLoopGroupHolder elgHolder;
    private final Bootstrap bootstrap;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public UnpooledNettyWsClient(EventLoopGroupHolder elgHolder, URI uri,
                                 HttpHeaders headers, @Nullable SslContext sslContext) {
        this.elgHolder = elgHolder;
        InetSocketAddress address = createUnresolved(uri.getHost(), Protocol.port(uri));
        val initializer = new WsChannelInitializer(sslContext, uri, headers, WsRecycler.CLOSE);
        this.bootstrap = elgHolder.acquireBootstrap(address).handler(initializer);
    }

    @Override
    public void open(WsClientHandler handler) {
        retryingOpen(handler, bootstrap::connect);
    }

    static void retryingOpen(WsClientHandler handler,
                             Supplier<Future<?>> channelSupplier) {
        NettyRetryingChannelSupplier.open(channelSupplier)
                .whenComplete((ch, cause) -> {
                    if (cause != null) {
                        handler.onError(cause);
                    } else {
                        try {
                            WsClientNettyHandler nettyHandler;
                            nettyHandler = (WsClientNettyHandler) ch.pipeline().get("ws");
                            if (nettyHandler == null) {
                                String msg = ch.isOpen()
                                        ? "\"ws\" handler missing from pipeline in ch="+ch
                                        : "Server closed the connection prematurely for ch="+ch;
                                handler.onError(new SparqlClientServerException(msg));
                            } else {
                                nettyHandler.delegate(handler);
                            }
                        } catch (Throwable t) {
                            handler.onError(t);
                        }
                    }
                });
    }

    @Override public void close() {
        if (closed.compareAndSet(false, true))
            elgHolder.release();
    }
}
