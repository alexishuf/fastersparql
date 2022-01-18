package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

public class UnPooledNettyHttpClient<H extends ReusableHttpClientInboundHandler>
        implements NettyHttpClient<H> {
    private static final String CONNECTION = "close";

    private final EventLoopGroupHolder groupHolder;
    private final Bootstrap bootstrap;
    private final ActiveChannelSet activeChannels = new ActiveChannelSet();
    private final String host;

    public UnPooledNettyHttpClient(EventLoopGroupHolder groupHolder, InetSocketAddress address,
                                   Supplier<? extends ReusableHttpClientInboundHandler> hFactory,
                                   @Nullable SslContext sslContext) {
        this.host = address.getHostString();
        EventLoopGroup group = groupHolder.acquire();
        try {
            this.bootstrap = new Bootstrap().remoteAddress(address).group(group)
                    .channel(groupHolder.transport().channelClass())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override protected void initChannel(SocketChannel ch) {
                            activeChannels.add(ch);
                            setupPipeline(ch, sslContext, hFactory);
                        }
                    });
            this.groupHolder = groupHolder;
        } catch (Throwable t) {
            groupHolder.release();
            throw t;
        }
    }

    static ReusableHttpClientInboundHandler
    setupPipeline(Channel ch, @Nullable SslContext sslContext,
                  Supplier<? extends ReusableHttpClientInboundHandler> hFactory) {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslContext != null)
            pipeline.addLast("ssl", sslContext.newHandler(ch.alloc()));
        pipeline.addLast("http", new HttpClientCodec());
        pipeline.addLast("decompress", new HttpContentDecompressor());
        ReusableHttpClientInboundHandler handler = hFactory.get();
        pipeline.addLast("handler", handler);
        return handler;
    }

    static <T extends ReusableHttpClientInboundHandler>
    void doRequestSetup(SocketChannel ch, String host, String connection,
                        HttpMethod method, CharSequence firstLine,
                        Throwing.@Nullable Function<ByteBufAllocator, ByteBuf> bodyGenerator,
                        @Nullable Setup<T> setup) throws Exception {
        HttpRequest req;
        if (bodyGenerator != null) {
            ByteBuf bb = bodyGenerator.apply(ch.alloc());
            req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, firstLine.toString(), bb);
            req.headers().set(HttpHeaderNames.CONTENT_LENGTH, bb.readableBytes());
        } else {
            req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, firstLine.toString());
        }
        HttpHeaders headers = req.headers();
        headers.set(HttpHeaderNames.HOST, host);
        headers.set(HttpHeaderNames.CONNECTION, connection);
        @SuppressWarnings("unchecked") T handler = (T) ch.pipeline().get("handler");
        if (setup != null)
            setup.setup(ch, req, handler);
        ch.writeAndFlush(req);
    }

    @Override
    public void request(HttpMethod method, CharSequence firstLine,
                        Throwing.@Nullable Function<ByteBufAllocator, ByteBuf> bodyGenerator,
                        @Nullable Setup<H> setup) {
        bootstrap.connect().addListener(f ->
                doRequestSetup((SocketChannel)((ChannelFuture)f).channel(), host, CONNECTION,
                               method, firstLine, bodyGenerator, setup));
    }

    @Override public void close() {
        activeChannels.close();
        groupHolder.release();
    }
}
