package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRetryingChannelSupplier;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

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
    void doRequestSetup(Channel ch, String host, String connection,
                        HttpMethod method, CharSequence firstLine,
                        Throwing.@Nullable Function<ByteBufAllocator, ByteBuf> bodyGenerator,
                        Setup<T> setup) {
        try {
            ByteBuf bb = null;
            if (bodyGenerator != null)
                bb = bodyGenerator.apply(ch.alloc());
            if (bb == null)
                bb = Unpooled.EMPTY_BUFFER;
            HttpRequest req = new DefaultFullHttpRequest(HTTP_1_1, method, firstLine.toString(), bb);
            HttpHeaders headers = req.headers();
            if (bb.readableBytes() > 0)
                headers.set(HttpHeaderNames.CONTENT_LENGTH, bb.readableBytes());
            headers.set(HttpHeaderNames.HOST, host);
            headers.set(HttpHeaderNames.CONNECTION, connection);
            @SuppressWarnings("unchecked") T handler = (T) ch.pipeline().get("handler");
            EventLoop loop = ch.eventLoop();
            if (handler == null)
                loop.schedule(() -> setupAndSend(ch, setup, req), 1, TimeUnit.MICROSECONDS);
            else
                loop.execute(() -> setupAndSend(ch, setup, req));
        } catch (Throwable t) {
            setup.requestError(t);
        }
    }

    private static <T extends ReusableHttpClientInboundHandler>
    void setupAndSend(Channel ch, Setup<T> setup, HttpRequest req)  {
        @SuppressWarnings("unchecked") T handler = (T) ch.pipeline().get("handler");
        assert handler != null;
        setup.setup(ch, req, handler);
        ch.writeAndFlush(req);
    }

    @Override
    public void request(HttpMethod method, CharSequence firstLine,
                        Throwing.@Nullable Function<ByteBufAllocator, ByteBuf> bodyGenerator,
                        Setup<H> setup) {
        NettyRetryingChannelSupplier.open(bootstrap::connect).whenComplete((ch, err) -> {
            if (err != null)
                setup.connectionError(err);
            else
                doRequestSetup(ch, host, CONNECTION, method, firstLine, bodyGenerator, setup);
        });
    }

    @Override public void close() {
        activeChannels.close();
        groupHolder.release();
    }
}
