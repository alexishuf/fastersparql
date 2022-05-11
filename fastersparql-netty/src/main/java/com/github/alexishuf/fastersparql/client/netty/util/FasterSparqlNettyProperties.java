package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.client.netty.NettySparqlClient;
import com.github.alexishuf.fastersparql.client.util.FasterSparqlProperties;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;

public class FasterSparqlNettyProperties extends FasterSparqlProperties {
    /* --- --- --- property names --- --- --- */

    public static final String TRUST_CERT_COLLECTION_FILE = "fastersparql.netty.ssl.trusted";
    public static final String START_TLS = "fastersparql.netty.ssl.starttls";
    public static final String OCSP = "fastersparql.netty.ssl.ocsp";
    public static final String POOL_ENABLE = "fastersparql.netty.pool.enable";
    public static final String POOL_FIFO = "fastersparql.netty.pool.fifo";
    public static final String ELG_SHARED = "fastersparql.netty.eventloopgroup.shared";
    public static final String ELG_KEEPALIVE = "fastersparql.netty.eventloopgroup.keepalive-seconds";
    public static final String WS_MAX_HTTP = "fastersparql.netty.ws.max-http";

    /* --- --- --- default values --- --- --- */

    public static final File DEF_TRUST_CERT_COLLECTION_FILE = null;
    public static final boolean DEF_START_TLS     = false;
    public static final boolean DEF_OCSP          = false;
    public static final boolean DEF_POOL_ENABLE   = true;
    public static final boolean DEF_POOL_FIFO     = false;
    public static final boolean DEF_ELG_SHARED    = true;
    public static final int     DEF_ELG_KEEPALIVE = 15;
    public static final int     DEF_WS_MAX_HTTP   = 8192;

    /* --- --- --- accessors --- --- --- */

    /**
     * A file with X.509 certificates in PEM format to be trusted by {@link NettySparqlClient}.
     *
     * See {@link SslContextBuilder#trustManager(File)}.
     *
     * @return a readable file with valid certificates or null
     * @throws IllegalArgumentException if there is no file at path, if the file is not
     *                                  readable or if the file does not contain syntactically
     *                                  correct X.509 certificates in PEM format
     */
    public static @Nullable File trustCertCollectionFile() {
        return readProperty(TRUST_CERT_COLLECTION_FILE, DEF_TRUST_CERT_COLLECTION_FILE, (src, val) -> {
            File file = new File(val);
            String expected = ", expected a file with X.509 certificates in PEM format";
            if (!file.exists())
                throw new IllegalArgumentException(src+"="+val+": file does not exist"+expected);
            if (!file.isDirectory())
                throw new IllegalArgumentException(src+"="+val+": file is a dir"+expected);
            if (!file.canRead())
                throw new IllegalArgumentException(src+"="+val+": no read permission");
            try {
                SslContextBuilder.forClient().trustManager(file);
            } catch (Throwable t) {
                throw new IllegalArgumentException(src+"="+val+": "+t);
            }
            return file;
        });
    }

    /**
     * Value for {@link SslContextBuilder#startTls(boolean)}, used by {@link NettySparqlClient}s.
     *
     * @return A bool. If not set, will return false, the default.
     */
    public static boolean startTls() { return readBoolean(START_TLS, DEF_START_TLS); }

    /**
     * Value for {@link SslContextBuilder#enableOcsp(boolean)} for use by {@link NettySparqlClient}s.
     *
     * @return A bool. If not set, will return false, the default.
     */
    public static boolean ocsp() { return readBoolean(OCSP, DEF_OCSP); }

    /**
     * Whether connections to an SPARQL endpoint should be pooled (if the server allows it).
     *
     * If pooling, requests will include the "connection: keep-alive" header and after a complete
     * response is handled, the connection returns to the pool, allowing a new request.
     * The pool size is unbounded.
     *
     * The default value is {@code true} (use a pool).
     *
     * @return whether a {@link NettySparqlClient} should pool its TCP connections.
     */
    public static boolean pool() { return readBoolean(POOL_ENABLE, DEF_POOL_ENABLE); }

    /**
     * If true, connection pools (see {@link FasterSparqlNettyProperties#pool()}) will operate
     * under a FIFO (First In, First Out) regime.
     *
     * The default is {@code false}, so pools operate on LIFO (Last-In, First-Out) regime. A
     * LIFO regime favors keeping a few busy connections making it easier for excess connections
     * to reach their timeout and die.
     *
     * @return whether connections on pools should be reused on a FIFO regime.
     */
    public static boolean poolFIFO() { return readBoolean(POOL_FIFO, DEF_POOL_FIFO); }

    /**
     * If {@code true} (the default), all {@link NettySparqlClient}s will share one single
     * {@link EventLoopGroup}. If false, each client will create its own.
     *
     * If shared, the {@link EventLoopGroup} will be shutdown once there is no
     * {@link NettySparqlClient} (indirectly) holding a reference to it. After shutdown a new
     * {@link NettySparqlClient} will create a new {@link EventLoopGroup} that
     * will again be shared if more {@link NettySparqlClient}s appear.
     *
     * Shutdown on zero references can be delayed using
     * {@link FasterSparqlNettyProperties#sharedEventLoopGroupKeepAliveSeconds()}
     *
     * The default is {@code true}
     *
     * @return Whether {@link NettySparqlClient}s should share a single reference-counted
     *         {@link EventLoopGroup}. The default is {@code true}.
     */
    public static boolean shareEventLoopGroup() {
        return readBoolean(ELG_SHARED, DEF_ELG_SHARED);
    }

    /**
     * If the last {@link NettySparqlClient} using a shared {@link EventLoopGroup}
     * is closed, wait this number of seconds before shutting down the
     * {@link EventLoopGroup}. If a new {@link NettySparqlClient} appears before the timeout, the
     * {@link EventLoopGroup} will gain a reference and shutdown will be cancelled.
     *
     * @return the number of seconds after the shared {@link EventLoopGroup} reaches zero
     * references to wait before shutting it down. The default is {@code 0}, making the shutdown
     * immediate.
     */
    public static int sharedEventLoopGroupKeepAliveSeconds() {
        return readPositiveInt(ELG_KEEPALIVE, DEF_ELG_KEEPALIVE);
    }

    /**
     * The maximum size, in bytes, of HTTP responses when performing a WebSocket handshake. This
     * limit does not apply to websocket messages that will be exchanged after the handshake.
     * The default is 8192 (8 KiB).
     */
    public static int wsMaxHttpResponse() {
        return readPositiveInt(WS_MAX_HTTP, DEF_WS_MAX_HTTP);
    }
}
