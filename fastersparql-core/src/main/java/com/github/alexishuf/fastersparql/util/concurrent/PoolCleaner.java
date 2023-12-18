package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.MIN_PRIORITY;
import static java.lang.Thread.onSpinWait;

public class PoolCleaner {
    private static final VarHandle LOCK;
    static {
        try {
            LOCK = MethodHandles.lookup().findVarHandle(PoolCleaner.class, "plainLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final Logger log = LoggerFactory.getLogger(PoolCleaner.class);
    private static final long CLEAN_INTERVAL_NS = 30_000_000_000L;

    public static final PoolCleaner INSTANCE = new PoolCleaner();

    private final Map<LeakyPool, Boolean> pools = new WeakHashMap<>();
    private final ArrayDeque<Thread> scanWaiters = new ArrayDeque<>();
    @SuppressWarnings("unused") private int plainLock;
    private final Thread thread;

    private PoolCleaner() {
        thread = new Thread(this::run, "PoolCleaner");
        thread.setPriority(MIN_PRIORITY);
        thread.setDaemon(true);
        thread.start();
    }

    public void monitor(LeakyPool pool) {
        while((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
        try {
            pools.put(pool, Boolean.TRUE);
        } finally { LOCK.setRelease(this, 0); }
    }

    /**
     * Starts a scan/purge of stale (leaky) references in all registered pools ASAP and
     * waits until such scan/purge completes.
     */
    public void sync() {
        Thread me = Thread.currentThread();
        while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
        try {
            scanWaiters.add(me);
        } finally { LOCK.setRelease(this, 0); }
        LockSupport.unpark(thread);
        while (true) {
            LockSupport.park(this);
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                if (!scanWaiters.contains(me)) break;
            } finally { LOCK.setRelease(this, 0); }
        }
    }

    protected void run() {
        ArrayDeque<Thread> unparkQueue = new ArrayDeque<>();
        List<LeakyPool> pools = new ArrayList<>();
        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                // read all registered pools
                while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
                try {
                    pools.addAll(this.pools.keySet());
                } finally { LOCK.setRelease(this, 0); }

                // clean refs of all registered pools
                for (LeakyPool pool : pools) {
                    try {
                        pool.cleanLeakyRefs();
                    } catch (Exception e) { log.warn("{}.cleanLeakyRefs failed: ", pool, e); }
                }

                // awake threads in sync()
                while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
                try {
                    for (Thread t; (t = scanWaiters.poll()) != null; ) unparkQueue.add(t);
                } finally { LOCK.setRelease(this, 0); }
                for (Thread t; (t = unparkQueue.poll()) != null; )
                    LockSupport.unpark(t);

                waitCleanIntervalOrSyncRequests();
            } catch (Exception e) {
                log.error("Unexpected error", e);
            }
        }
    }

    private void waitCleanIntervalOrSyncRequests() {
        long waitNs = CLEAN_INTERVAL_NS, waitStart = Timestamp.nanoTime();
        while (waitNs > 0) {
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                if (!scanWaiters.isEmpty())
                    break;
            } finally { LOCK.setRelease(this, 0); }
            LockSupport.parkNanos(this, waitNs);
            waitNs = CLEAN_INTERVAL_NS - (Timestamp.nanoTime()-waitStart);
        }
    }
}
