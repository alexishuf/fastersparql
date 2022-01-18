package com.github.alexishuf.fastersparql.client.netty.http;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.IdentityHashMap;

@Slf4j
public class ActiveChannelSet implements AutoCloseable {
    private boolean closed;
    private final IdentityHashMap<Channel, Channel> active = new IdentityHashMap<>();

    public synchronized void add(Channel ch) {
        active.put(ch, ch);
        ch.closeFuture().addListener(f -> remove(ch));
    }

    public synchronized void remove(Channel ch) {
        active.remove(ch);
    }

    @Override public void close() {
        ArrayList<Channel> copy;
        synchronized (this) {
            if (closed)
                return;
            closed = true;
            copy = new ArrayList<>(active.keySet());
            active.clear();
        }
        if (!copy.isEmpty()) {
            log.info("Closing {} channels on {}.close()", copy.size(), this);
            for (Channel ch : copy) {
                log.debug("Closing {} on {}.close()", ch, this);
                try {
                    ch.close();
                } catch (Throwable t) {
                    log.error("Unexpected {} from {}.close()", t.getClass().getSimpleName(), ch, t);
                }
            }
        }
    }
}
