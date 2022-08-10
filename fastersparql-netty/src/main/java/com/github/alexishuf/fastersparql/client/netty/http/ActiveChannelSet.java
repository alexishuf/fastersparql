package com.github.alexishuf.fastersparql.client.netty.http;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;

@SuppressWarnings("UnusedReturnValue")
public class ActiveChannelSet implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ActiveChannelSet.class);

    private final String name;
    private boolean closed;
    private final IdentityHashMap<Channel, Channel> open = new IdentityHashMap<>();
    private final IdentityHashMap<Channel, Channel> active = new IdentityHashMap<>();

    public ActiveChannelSet(String name) { this.name = name; }

    public synchronized ActiveChannelSet add(Channel ch) {
        open.put(ch, ch);
        ch.closeFuture().addListener(f -> remove(ch));
        return this;
    }

    public synchronized Channel   setActive(Channel ch) { active.put(ch, ch); return ch; }
    public synchronized Channel setInactive(Channel ch) { active.remove(ch);  return ch; }
    public synchronized void         remove(Channel ch) { open.remove(setInactive(ch)); }

    @Override public void close() {
        ArrayList<Channel> copy;
        int activeCount;
        synchronized (this) {
            if (closed)
                return;
            closed = true;
            copy = new ArrayList<>(open.keySet());
            activeCount = active.size();
            open.clear();
        }
        if (activeCount > 0) {
            log.info("Closing {} active channels on {}.close(), this will cause termination " +
                     "of downstream publishers", copy.size(), this);
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

    @Override public String toString() {
        return "ActiveChannelSet["+name+"]";
    }
}
