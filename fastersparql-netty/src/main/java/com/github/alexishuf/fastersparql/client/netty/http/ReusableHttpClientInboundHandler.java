package com.github.alexishuf.fastersparql.client.netty.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;

public interface ReusableHttpClientInboundHandler extends ChannelInboundHandler {
    /**
     * When the handler finishes handling a full response, it must call the given {@link Runnable}.
     *
     * <p>If the {@link Channel} should not be reused (e.g. corrupted state or failing server),
     * then instead of calling this runnable, the handler should trigger a {@link Channel#close()}.
     * </p>
     *
     * <p>If this method is not called on the {@link ReusableHttpClientInboundHandler}, then it is
     * handling a non-pooled {@link Channel}.</p>
     *
     * @param runnable {@link Runnable} to call when the handler finishes handling a response.
     */
    void onResponseEnd(Runnable runnable);
}
