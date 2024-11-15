package com.github.alexishuf.fastersparql.client.netty.util;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties.sharedEventLoopGroupKeepAliveSeconds;
import static com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties.sharedEventLoopGroupPhysAffnity;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SharedEventLoopGroupHolder {
    private static final Logger log = LoggerFactory.getLogger(SharedEventLoopGroupHolder.class);
    private static final SharedEventLoopGroupHolder INSTANCE = new SharedEventLoopGroupHolder();

    private @MonotonicNonNull EventLoopGroupHolder elgHolder;
    private boolean warnedKeepAlive = false, warnedPhysAffinity;

    public static EventLoopGroupHolder get() {
        return INSTANCE.doGet();
    }

    private synchronized EventLoopGroupHolder doGet() {
        int keepAliveSeconds = sharedEventLoopGroupKeepAliveSeconds();
        if (elgHolder == null) {
            int threads = Runtime.getRuntime().availableProcessors();
            boolean physAffinity = sharedEventLoopGroupPhysAffnity();
            if (physAffinity && !warnedPhysAffinity) {
                warnedPhysAffinity = true;
                log.info("Physical core affinity is enabled for \"shared\" netty ELG");
            }
            elgHolder = new EventLoopGroupHolder("shared",
                    null, keepAliveSeconds, SECONDS, threads,
                    physAffinity);
        } else if (elgHolder.keepAlive(SECONDS) != keepAliveSeconds && !warnedKeepAlive) {
            log.warn("sharedEventLoopGroupKeepAliveSeconds={} will not be honored as " +
                     "shared EventLoopGroupHolder has already been instantiated",
                     keepAliveSeconds);
            warnedKeepAlive = true;
        }
        return elgHolder;
    }
}
