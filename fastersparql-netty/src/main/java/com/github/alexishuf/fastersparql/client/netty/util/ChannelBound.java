package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.util.concurrent.JournalNamed;
import io.netty.channel.Channel;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface ChannelBound extends JournalNamed {
    @Nullable Channel channelOrLast();
    void setChannel(Channel ch);
}
