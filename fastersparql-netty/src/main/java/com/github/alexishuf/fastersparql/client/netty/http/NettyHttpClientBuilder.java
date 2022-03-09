package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.FasterSparqlNettyProperties;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.net.ssl.SSLException;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static io.netty.handler.ssl.SslContextBuilder.forClient;
import static java.util.concurrent.TimeUnit.SECONDS;

@Data @Accessors(fluent = true, chain = true)
@Slf4j
public final class NettyHttpClientBuilder {
    private static Lock SHARED_HOLDER_LOCK = new ReentrantLock();
    private static @MonotonicNonNull EventLoopGroupHolder SHARED_HOLDER;

    private boolean shareEventLoopGroup = FasterSparqlNettyProperties.shareEventLoopGroup();
    private int sharedEventLoopGroupKeepAliveSeconds =
            FasterSparqlNettyProperties.sharedEventLoopGroupKeepAliveSeconds();
    private boolean pooled = FasterSparqlNettyProperties.pool();
    private boolean poolFIFO = FasterSparqlNettyProperties.poolFIFO();
    private boolean ocsp = FasterSparqlNettyProperties.ocsp();
    private boolean startTls = FasterSparqlNettyProperties.startTls();
    private @Nullable File trustCertCollectionFile =
            FasterSparqlNettyProperties.trustCertCollectionFile();

    private EventLoopGroupHolder elgHolder() {
        if (shareEventLoopGroup) {
            SHARED_HOLDER_LOCK.lock();
            try {
                if (SHARED_HOLDER == null) {
                    SHARED_HOLDER = EventLoopGroupHolder.builder()
                            .keepAlive(sharedEventLoopGroupKeepAliveSeconds)
                            .keepAliveTimeUnit(SECONDS).build();
                } else if (SHARED_HOLDER.keepAlive(SECONDS) != sharedEventLoopGroupKeepAliveSeconds) {
                    log.warn("sharedEventLoopGroupKeepAliveSeconds={} will not be honored as " +
                             "shared EventLoopGroupHolder has already been instantiated",
                             sharedEventLoopGroupKeepAliveSeconds);
                }
            } finally {
                SHARED_HOLDER_LOCK.unlock();
            }
            return SHARED_HOLDER;
        } else {
            return EventLoopGroupHolder.builder().keepAlive(0).build();
        }
    }

    /**
     * Build a {@link NettyHttpClient} with the builder settings
     *
     * @param protocol The protocol to use, either HTTP or HTTPS
     * @param address address and port of the remote server. Ideally this should've been
     *                created from a hostname and not from the textual representation of the
     *                IP address. The {@link InetSocketAddress#getHostString()} will be used
     *                with the HTTP {@code Host} header.
     * @param factory A factory for response handlers
     * @param <H> The response handler type
     * @return A new {@link NettyHttpClient}, whose ownership is given to the caller
     * @throws SSLException If protocol is HTTPS and something goes wrong on {@link SslContext}
     *         initialization. Such exceptions are usually configuration (or environment) issues.
     */
    public <H extends ReusableHttpClientInboundHandler> NettyHttpClient<H>
    build(@lombok.NonNull Protocol protocol,
          @lombok.NonNull InetSocketAddress address,
          @lombok.NonNull Supplier<H> factory) throws SSLException {
        SslContext sslContext = null;
        if (protocol == Protocol.HTTPS) {
            SslContextBuilder sslBuilder = forClient().enableOcsp(ocsp).startTls(startTls);
            if (trustCertCollectionFile != null)
                sslBuilder.trustManager(trustCertCollectionFile);
            sslContext = sslBuilder.build();
        }
        if (pooled) {
            return new PooledNettyHttpClient<>(elgHolder(), address, factory, sslContext);
        } else {
            return new UnPooledNettyHttpClient<>(elgHolder(), address, factory, sslContext);
        }
    }
}
