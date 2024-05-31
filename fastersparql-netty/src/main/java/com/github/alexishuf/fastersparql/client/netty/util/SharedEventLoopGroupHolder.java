package com.github.alexishuf.fastersparql.client.netty.util;

import com.github.alexishuf.fastersparql.emit.async.ThreadPoolsPartitioner;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.github.alexishuf.fastersparql.client.netty.util.FSNettyProperties.sharedEventLoopGroupKeepAliveSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SharedEventLoopGroupHolder {
    static {
        ThreadPoolsPartitioner.registerPartition(SharedEventLoopGroupHolder.class.getSimpleName());
    }
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
            int threads = 2*ThreadPoolsPartitioner.partitionSize();
            var affinity = ThreadPoolsPartitioner.nextLogicalCoreSet();
            elgHolder = new EventLoopGroupHolder("shared",
                    null, keepAliveSeconds, SECONDS, threads, affinity);
        } else if (elgHolder.keepAlive(SECONDS) != keepAliveSeconds && !warnedKeepAlive) {
            log.warn("sharedEventLoopGroupKeepAliveSeconds={} will not be honored as " +
                     "shared EventLoopGroupHolder has already been instantiated",
                     keepAliveSeconds);
            warnedKeepAlive = true;
        }
        return elgHolder;
    }
}
