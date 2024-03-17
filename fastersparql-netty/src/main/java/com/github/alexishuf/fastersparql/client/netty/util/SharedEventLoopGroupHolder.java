package com.github.alexishuf.fastersparql.client.netty.util;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.FSProperties.nettyEventLoopThreads;
import static com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties.sharedEventLoopGroupKeepAliveSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SharedEventLoopGroupHolder {
    private static final Logger log = LoggerFactory.getLogger(SharedEventLoopGroupHolder.class);
    private static final SharedEventLoopGroupHolder INSTANCE = new SharedEventLoopGroupHolder();

    private @MonotonicNonNull EventLoopGroupHolder elgHolder;
    private boolean warnedKeepAlive = false;

    public static EventLoopGroupHolder get() {
        return INSTANCE.doGet();
    }

    private synchronized EventLoopGroupHolder doGet() {
        int keepAliveSeconds = sharedEventLoopGroupKeepAliveSeconds();
        if (elgHolder == null) {
            int threads = nettyEventLoopThreads();
            elgHolder = new EventLoopGroupHolder("shared",
                    null, keepAliveSeconds, SECONDS, threads);
        } else if (elgHolder.keepAlive(SECONDS) != keepAliveSeconds && !warnedKeepAlive) {
            log.warn("sharedEventLoopGroupKeepAliveSeconds={} will not be honored as " +
                     "shared EventLoopGroupHolder has already been instantiated",
                     keepAliveSeconds);
            warnedKeepAlive = true;
        }
        return elgHolder;
    }
}
