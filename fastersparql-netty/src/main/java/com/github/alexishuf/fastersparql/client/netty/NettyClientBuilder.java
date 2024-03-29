package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.netty.http.NettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.PooledNettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.http.UnPooledNettyHttpClient;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.FasterSparqlNettyProperties;
import com.github.alexishuf.fastersparql.client.netty.util.SharedEventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.impl.PooledNettyWsClient;
import com.github.alexishuf.fastersparql.client.netty.ws.impl.UnpooledNettyWsClient;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.netty.handler.ssl.SslContextBuilder.forClient;

@SuppressWarnings("unused")
public final class NettyClientBuilder {
    private static final Logger log = LoggerFactory.getLogger(NettyClientBuilder.class);

    private boolean shareEventLoopGroup = FasterSparqlNettyProperties.shareEventLoopGroup();
    private boolean pooled = FasterSparqlNettyProperties.pool();
    private boolean poolFIFO = FasterSparqlNettyProperties.poolFIFO();
    private boolean ocsp = FasterSparqlNettyProperties.ocsp();
    private boolean startTls = FasterSparqlNettyProperties.startTls();
    private @Nullable File trustCertCollectionFile =
            FasterSparqlNettyProperties.trustCertCollectionFile();

    public boolean shareEventLoopGroup() { return shareEventLoopGroup; }
    public boolean pooled() { return pooled; }
    public boolean poolFIFO() { return poolFIFO; }
    public boolean ocsp() { return ocsp; }
    public boolean startTls() { return startTls; }
    public @Nullable File trustCertCollectionFile() { return trustCertCollectionFile; }

    public NettyClientBuilder shareEventLoopGroup(boolean value)  { shareEventLoopGroup = value; return this; }
    public NettyClientBuilder pooled(boolean value)               { pooled = value; return this; }
    public NettyClientBuilder poolFIFO(boolean value)             { poolFIFO = value; return this; }
    public NettyClientBuilder ocsp(boolean value)                 { ocsp = value; return this; }
    public NettyClientBuilder startTls(boolean value)             { startTls = value; return this; }
    public NettyClientBuilder trustCertCollectionFile(File value) { trustCertCollectionFile = value; return this; }

    private EventLoopGroupHolder elgHolder() {
        if (shareEventLoopGroup) {
            return SharedEventLoopGroupHolder.get();
        } else {
            return new EventLoopGroupHolder(null, 0, TimeUnit.SECONDS);
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
    buildHTTP(Protocol protocol,
              InetSocketAddress address,
              Supplier<H> factory) throws SSLException {
        if (protocol.isWebSocket())
            throw new IllegalArgumentException("WS(S) not supported by buildHTTP");
        SslContext sslContext = buildSslContext(protocol);
        if (pooled)
            return new PooledNettyHttpClient<>(elgHolder(), address, factory, poolFIFO, sslContext);
        else
            return new UnPooledNettyHttpClient<>(elgHolder(), address, factory, sslContext);
    }

    private @Nullable SslContext buildSslContext(@NonNull Protocol protocol) throws SSLException {
        SslContext sslContext = null;
        if (protocol.needsSsl()) {
            SslContextBuilder sslBuilder = forClient().enableOcsp(ocsp).startTls(startTls);
            if (trustCertCollectionFile != null)
                sslBuilder.trustManager(trustCertCollectionFile);
            sslContext = sslBuilder.build();
        }
        return sslContext;
    }

    /**
     * Build a {@link NettyWsClient} that will open a WebSocket session on the given URI.
     *
     * @param protocol Either {@link Protocol#WS} or {@link Protocol#WSS}
     * @param uri the URI where the HTTP connection will be upgraded to WebSocket.
     * @param headers headers to use on the initial HTTP request.
     *
     * @return a {@link NettyWsClient} ready to use.
     * @throws SSLException If protocol is {@link Protocol#WSS} and something goes wrong
     *                      when creating an {@link SslContext}.
     */
    public NettyWsClient buildWs(Protocol protocol, URI uri, HttpHeaders headers) throws SSLException {
        if (!protocol.isWebSocket())
            throw new IllegalArgumentException("WS(S) not supported by buildWs");
        SslContext sslContext = buildSslContext(protocol);
        if (pooled)
            return new PooledNettyWsClient(elgHolder(), uri, headers, poolFIFO, sslContext);
        else
            return new UnpooledNettyWsClient(elgHolder(), uri, headers, sslContext);
    }
}
