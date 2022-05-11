package com.github.alexishuf.fastersparql.client.netty.util;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import static com.github.alexishuf.fastersparql.client.netty.util.FasterSparqlNettyProperties.sharedEventLoopGroupKeepAliveSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class SharedEventLoopGroupHolder {
    private static final SharedEventLoopGroupHolder INSTANCE = new SharedEventLoopGroupHolder();

    private @MonotonicNonNull EventLoopGroupHolder elgHolder;

    public static EventLoopGroupHolder get() {
        return INSTANCE.doGet();
    }

    private EventLoopGroupHolder doGet() {
        int keepAliveSeconds = sharedEventLoopGroupKeepAliveSeconds();
        if (elgHolder == null) {
            elgHolder = EventLoopGroupHolder.builder()
                    .keepAliveTimeUnit(SECONDS)
                    .keepAlive(keepAliveSeconds).build();
        } else if (elgHolder.keepAlive(SECONDS) != keepAliveSeconds) {
            log.warn("sharedEventLoopGroupKeepAliveSeconds={} will not be honored as " +
                     "shared EventLoopGroupHolder has already been instantiated",
                     keepAliveSeconds);
        }
        return elgHolder;
    }
}
