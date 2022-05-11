package com.github.alexishuf.fastersparql.client.netty.ws.impl;

import com.github.alexishuf.fastersparql.client.netty.util.FasterSparqlNettyProperties;
import com.github.alexishuf.fastersparql.client.netty.ws.WsClientNettyHandler;
import com.github.alexishuf.fastersparql.client.netty.ws.WsRecycler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;

@RequiredArgsConstructor
class WsChannelInitializer extends ChannelInitializer<Channel> {
    private final int maxHttp = FasterSparqlNettyProperties.wsMaxHttpResponse();
    private final @Nullable SslContext sslContext;
    private final URI uri;
    private final HttpHeaders headers;
    private final WsRecycler recycler;

    @Override protected void initChannel(Channel ch) {
        ChannelPipeline pipe = ch.pipeline();
        if (sslContext != null)
            pipe.addLast("ssl", sslContext.newHandler(ch.alloc()));
        pipe.addLast("http", new HttpClientCodec());
        pipe.addLast("aggregator", new HttpObjectAggregator(maxHttp));
        pipe.addLast("comp", WebSocketClientCompressionHandler.INSTANCE);
        pipe.addLast("ws", new WsClientNettyHandler(uri, headers, recycler));
    }
}
