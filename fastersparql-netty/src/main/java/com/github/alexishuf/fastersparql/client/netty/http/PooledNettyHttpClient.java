package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.FasterSparqlNettyProperties;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.SslContext;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.client.netty.http.UnPooledNettyHttpClient.doRequestSetup;
import static com.github.alexishuf.fastersparql.client.netty.http.UnPooledNettyHttpClient.setupPipeline;

public class PooledNettyHttpClient<H extends ReusableHttpClientInboundHandler>
        implements NettyHttpClient<H> {
    private static final Logger log = LoggerFactory.getLogger(PooledNettyHttpClient.class);
    private static final String CONNECTION = "keep-alive";

    private final EventLoopGroupHolder groupHolder;
    private final String host;
    private final SimpleChannelPool pool;
    private final ActiveChannelSet activeChannels = new ActiveChannelSet();

    public PooledNettyHttpClient(EventLoopGroupHolder groupHolder,
                                 InetSocketAddress address,
                                 Supplier<? extends ReusableHttpClientInboundHandler> hFactory,
                                 @Nullable SslContext sslContext) {
        EventLoopGroup group = groupHolder.acquire();
        try {
            Bootstrap bootstrap = new Bootstrap().group(group).remoteAddress(address)
                    .channel(groupHolder.transport().channelClass());
            this.host = address.getHostString();
            boolean lifo = !FasterSparqlNettyProperties.poolFIFO();
            this.pool = new SimpleChannelPool(bootstrap, new AbstractChannelPoolHandler() {
                @Override public void channelAcquired(Channel ch) {
                    log.trace("channelAcquired({})", ch);
                }

                @Override public void channelCreated(Channel ch) {
                    log.trace("channelCreated({})", ch);
                    activeChannels.add(ch);
                    setupPipeline(ch, sslContext, hFactory).onResponseEnd(() -> release(ch));
                }
            }, ChannelHealthChecker.ACTIVE, true, lifo);
            this.groupHolder = groupHolder;
        } catch (Throwable t) {
            groupHolder.release();
            throw t;
        }
    }

    private void release(Channel ch) {
        pool.release(ch);
    }

    @Override
    public void request(HttpMethod method, CharSequence firstLine,
                        Throwing.@Nullable Function<ByteBufAllocator, ByteBuf> bodyGenerator,
                        @Nullable Setup<H> setup) {
        pool.acquire().addListener(f -> doRequestSetup(f, host, CONNECTION,
                                                       method, firstLine, bodyGenerator, setup));
    }

    @Override public void close() {
        activeChannels.close();
        groupHolder.release();
    }
}
