package com.github.alexishuf.fastersparql.client.netty.util;

import io.netty.channel.Channel;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface ChannelBound {
    @Nullable Channel channel();
    String journalName();
}
