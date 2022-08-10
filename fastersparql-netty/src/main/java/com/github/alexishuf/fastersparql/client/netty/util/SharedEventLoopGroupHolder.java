package com.github.alexishuf.fastersparql.client.netty.util;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.client.netty.util.FasterSparqlNettyProperties.sharedEventLoopGroupKeepAliveSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SharedEventLoopGroupHolder {
    private static final Logger log = LoggerFactory.getLogger(SharedEventLoopGroupHolder.class);
    private static final SharedEventLoopGroupHolder INSTANCE = new SharedEventLoopGroupHolder();

    private @MonotonicNonNull EventLoopGroupHolder elgHolder;

    public static EventLoopGroupHolder get() {
        return INSTANCE.doGet();
    }

    private synchronized EventLoopGroupHolder doGet() {
        int keepAliveSeconds = sharedEventLoopGroupKeepAliveSeconds();
        if (elgHolder == null) {
            elgHolder = new EventLoopGroupHolder(null, keepAliveSeconds, SECONDS);
        } else if (elgHolder.keepAlive(SECONDS) != keepAliveSeconds) {
            log.warn("sharedEventLoopGroupKeepAliveSeconds={} will not be honored as " +
                     "shared EventLoopGroupHolder has already been instantiated",
                     keepAliveSeconds);
        }
        return elgHolder;
    }
}
