package com.github.alexishuf.fastersparql.client.netty.http;

import com.github.alexishuf.fastersparql.client.netty.handler.ReusableHttpClientInboundHandler;
import com.github.alexishuf.fastersparql.client.util.Throwing;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Utility that allows executing HTTP(S) requests using Netty.
 *
 * Implementations will hold an {@link io.netty.channel.EventLoopGroup} and possibly a
 * connection pool. Each {@link NettyHttpClient} targets a single remote server and every
 * {@link SocketChannel} will always end its pipeline with an
 * {@link ReusableHttpClientInboundHandler} implementation {@code H}.
 *
 * Since a {@link NettyHttpClient} may pool its {@link SocketChannel}s, An {@code H} instance
 * may handle more than one request (but the {@link SocketChannel} will still be the same).
 * If that is the case, {@link ReusableHttpClientInboundHandler#onResponseEnd(Runnable)} will
 * be called before the handler processes its first response.
 *
 * @param <H> the application-specific HTTP response handler.
 */
public interface NettyHttpClient<H extends ReusableHttpClientInboundHandler> extends AutoCloseable {
    interface Setup<H2 extends ReusableHttpClientInboundHandler> {
        void setup(Channel ch, HttpRequest request, H2 handler);
        void connectionError(Throwable cause);
        void requestError(Throwable cause);
    }

    /**
     * Connect to the remote server and send an HTTP request of the given {@code method} using
     * the {@code firstLine} as the target of that method.
     *
     * A body may be added to the request by providing a {@code bodyGenerator} parameter.
     *
     * Additional setup to change the behavior of the {@link SocketChannel}-specific handler for
     * inbound data can be done using the {@code setup} parameter.
     * {@link Setup#setup(Channel, HttpRequest, ReusableHttpClientInboundHandler)} will be
     * called before request data is sent over the wire. Thus, the request can be changed and the
     * handler will be ready once response data arrives.
     *
     * @param method the HTTP method of the request
     * @param firstLine the target of the method. This should be only the path and query
     *                  segments of the whole URI. If this happens to be a full URI
     * @param bodyGenerator a function that fills a {@link ByteBuf} taken from the given allocator
     *                      with the bytes of the request body. If this is non-null, remember to
     *                      set Content-Type on {@code setup}. The Content-Length request header
     *                      will be set to the length of the returned {@link ByteBuf}
     * @param setup A function that receives the {@link SocketChannel}, the {@link HttpRequest}
     *              and the {@code H} handler unique to the {@link SocketChannel}. This function
     *              can mutate all three parameters it receives, as it will run before the request
     *              is sent through netty.
     */
    void request(HttpMethod method, CharSequence firstLine,
                 Throwing.@Nullable Function<ByteBufAllocator, ByteBuf> bodyGenerator,
                 @Nullable Setup<H> setup);

    /**
     * Releases resources internally held by this instance, such as pools and
     * non-shared {@link io.netty.channel.EventLoopGroup}s.
     *
     * This method should not block, and if necessary, cleanup may continue on background threads.
     *
     * No exceptions should be thrown.
     */
    @Override void close();
}
