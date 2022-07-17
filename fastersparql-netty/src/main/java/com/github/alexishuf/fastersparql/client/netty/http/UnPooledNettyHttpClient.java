package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRetryingChannelSupplier;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import lombok.val;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class UnPooledNettyHttpClient<H extends ReusableHttpClientInboundHandler>
        implements NettyHttpClient<H> {
    private static final String CONNECTION = "close";

    private final EventLoopGroupHolder groupHolder;
    private final Bootstrap bootstrap;
    private final ActiveChannelSet activeChannels;
    private final String host;

    public UnPooledNettyHttpClient(EventLoopGroupHolder groupHolder, InetSocketAddress address,
                                   Supplier<? extends ReusableHttpClientInboundHandler> hFactory,
                                   @Nullable SslContext sslContext) {
        this.activeChannels = new ActiveChannelSet(address.toString());
        this.host = address.getHostString();
        val initializer = new ChannelInitializer<SocketChannel>() {
            @Override protected void initChannel(SocketChannel ch) {
                activeChannels.add(ch).setActive(ch);
                setupPipeline(ch, sslContext, hFactory);
                ch.closeFuture().addListener(ignored -> activeChannels.setInactive(ch));
            }
        };
        this.groupHolder = groupHolder;
        this.bootstrap = groupHolder.acquireBootstrap(address).handler(initializer);
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
            setupAndSend(ch, setup, req);
        } catch (Throwable t) {
            setup.requestError(t);
        }
    }

    private static <T extends ReusableHttpClientInboundHandler>
    void setupAndSend(Channel ch, Setup<T> setup, HttpRequest req)  {
        if (ch.eventLoop().inEventLoop()) {
            @SuppressWarnings("unchecked") T handler = (T) ch.pipeline().get("handler");
            if (handler == null) {
                String msg = ch.isOpen() ? "\"handler\" missing from pipeline of channel="+ch
                                         : "Server closed the connection for channel="+ch;
                setup.connectionError(new SparqlClientServerException(msg));
            } else {
                setup.setup(ch, req, handler);
                ch.writeAndFlush(req);
            }
        } else {
            ch.eventLoop().execute(() -> setupAndSend(ch, setup, req));
        }
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
