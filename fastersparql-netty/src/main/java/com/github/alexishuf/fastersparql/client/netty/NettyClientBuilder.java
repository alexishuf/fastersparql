package com.github.alexishuf.fastersparql.client.netty;

import com.github.alexishuf.fastersparql.client.model.Protocol;
import com.github.alexishuf.fastersparql.client.netty.http.*;
import com.github.alexishuf.fastersparql.client.netty.util.EventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties;
import com.github.alexishuf.fastersparql.client.netty.util.SharedEventLoopGroupHolder;
import com.github.alexishuf.fastersparql.client.netty.ws.NettyWsClient;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.netty.handler.ssl.SslContextBuilder.forClient;

@SuppressWarnings("unused")
public final class NettyClientBuilder {
    private static final Logger log = LoggerFactory.getLogger(NettyClientBuilder.class);

    private boolean shareEventLoopGroup = FSNettyProperties.shareEventLoopGroup();
    private boolean pooled = FSNettyProperties.pool();
    private boolean poolFIFO = FSNettyProperties.poolFIFO();
    private boolean ocsp = FSNettyProperties.ocsp();
    private boolean startTls = FSNettyProperties.startTls();
    private @Nullable File trustCertCollectionFile =
            FSNettyProperties.trustCertCollectionFile();

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
     * Create a new {@link NettyHttpClient} with current builder settings.
     *
     * @param uri Base URI of the host where connections will be made. The path, query and
     *            fragment portions of the URI will be ignored (scheme, host and port will be used).
     *            The host may be a IPv4/IPv6 address or a DNS hostname that will be resolved at
     *            connection time.
     * @param factory {@link Supplier} that yields a new {@link ReusableHttpClientInboundHandler}
     *                instances when requested.
     * @return a new {@link NettyHttpClient}
     * @throws SSLException If the URI scheme is https and the builder SSL settings could not be
     *                      loaded
     */
    public NettyHttpClient
    buildHTTP(String uri,
              Supplier<? extends ReusableHttpClientInboundHandler > factory) throws SSLException {
        if (uri.startsWith("ws://"))
            throw new IllegalArgumentException("WS(S) not supported by buildHTTP");
        var sslContext = uri.startsWith("https://") ? buildSslContext() : null;
        return new NettyHttpClient(elgHolder(), uri, factory, pooled, poolFIFO, sslContext);
    }

    private SslContext buildSslContext() throws SSLException {
        SslContextBuilder sslBuilder = forClient().enableOcsp(ocsp).startTls(startTls);
        if (trustCertCollectionFile != null)
            sslBuilder.trustManager(trustCertCollectionFile);
        return sslBuilder.build();
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
        SslContext sslContext = protocol.needsSsl() ? buildSslContext() : null;
        return new NettyWsClient(elgHolder(), uri, headers, pooled, poolFIFO, sslContext);
    }
}
