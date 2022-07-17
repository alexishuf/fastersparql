package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.NettyRetryingChannelSupplier;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
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
    private final ActiveChannelSet activeChannels;

    public PooledNettyHttpClient(EventLoopGroupHolder groupHolder,
                                 InetSocketAddress address,
                                 Supplier<? extends ReusableHttpClientInboundHandler> hFactory,
                                 boolean poolFIFO,
                                 @Nullable SslContext sslContext) {
        activeChannels = new ActiveChannelSet(address.toString());
        Bootstrap bootstrap = groupHolder.acquireBootstrap(address);
        try {
            this.host = address.getHostString();
            boolean lifo = !poolFIFO;
            this.pool = new SimpleChannelPool(bootstrap, new AbstractChannelPoolHandler() {
                @Override public void channelAcquired(Channel ch) {
                    log.trace("channelAcquired({})", ch);
                    activeChannels.setActive(ch);
                }
                @Override public void channelReleased(Channel ch) {
                    activeChannels.setInactive(ch);
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
        NettyRetryingChannelSupplier.open(pool::acquire).whenComplete((ch, err) -> {
            if (err != null) {
                if (setup != null) setup.connectionError(err);
            } else {
                doRequestSetup(ch, host, CONNECTION, method, firstLine, bodyGenerator, setup);
            }
        });
    }

    @Override public void close() {
        activeChannels.close();
        groupHolder.release();
    }
}
