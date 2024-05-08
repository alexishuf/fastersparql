package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.FSProperties;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PoolStats {
    public static final boolean ENABLED = FSProperties.poolStats();
    private static final Queue<StatsPool> pools = new ConcurrentLinkedQueue<>();
    private static final Thread thread;
    static {
        thread = new Thread(PoolStats::run, "PoolStats");
        thread.setDaemon(true);
        thread.setPriority(Thread.NORM_PRIORITY-1);
        if (ENABLED)
            thread.start();
    }

    public static void monitor(StatsPool pool) {
        if (!pools.contains(pool))
            pools.add(pool);
    }

    private static void run() {
        var event = new PoolStatsEvent();
        while (true) {
            Async.uninterruptibleSleep(200);
            for (StatsPool p : pools)
                event.fillAndCommit(p);
        }
    }
}
