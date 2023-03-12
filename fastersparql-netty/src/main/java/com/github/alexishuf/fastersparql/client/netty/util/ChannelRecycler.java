package com.github.alexishuf.fastersparql.client.netty.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundInvoker;

@FunctionalInterface
public interface ChannelRecycler {
    /** Allow the given {@link Channel} to be reused when issuing new requests.
     *
     * <p>The caller must be sure that issuing new requests is safe. Implementations are allowed
     * to not attempt such reuse and simply call {@code channel.close()}.</p> */
    void recycle(Channel channel);

    /** A {@link ChannelRecycler} that simply closes the channel. */
    ChannelRecycler CLOSE = ChannelOutboundInvoker::close;
}
