package com.github.alexishuf.fastersparql.util.concurrent;

import org.jctools.queues.MessagePassingQueue.Consumer;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.MIN_PRIORITY;
import static java.lang.Thread.onSpinWait;

public class PoolCleaner implements BackgroundTask {
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
    @SuppressWarnings("unused") private int plainLock;
    private final MpscUnboundedAtomicArrayQueue<Object> sync
            = new MpscUnboundedAtomicArrayQueue<>(32);
    private final Thread thread;

    private PoolCleaner() {
        thread = new Thread(this::run, "PoolCleaner");
        thread.setPriority(MIN_PRIORITY);
        thread.setDaemon(true);
        BackgroundTasks.register(this);
        thread.start();
    }

    public static void monitor(LeakyPool pool) {
        INSTANCE.monitor0(pool);
    }

    public void monitor0(LeakyPool pool) {
        while((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
        try {
            pools.put(pool, Boolean.TRUE);
        } finally { LOCK.setRelease(this, 0); }
    }

    @Override public void sync(CountDownLatch latch) {
        while (!sync.offer(latch))
            Thread.yield();
        Unparker.unpark(thread);
    }

    protected void run() {
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
                sync.drain(SYNC_HANDLER);

                waitCleanIntervalOrSyncRequests();
            } catch (Exception e) {
                log.error("Unexpected error", e);
            }
        }
    }

    private static final class SyncHandler implements Consumer<Object> {
        @Override public void accept(Object o) {
            if      (o instanceof Thread         t) LockSupport.unpark(t);
            else if (o instanceof CountDownLatch l) l.countDown();
            else                                    log.error("Unexpected sync object: {}", o);
        }
        @Override public String toString() {return "PoolCleaner.SyncHandler";}
    }
    private static final SyncHandler SYNC_HANDLER = new SyncHandler();


    private void waitCleanIntervalOrSyncRequests() {
        long waitNs = CLEAN_INTERVAL_NS, waitStart = Timestamp.nanoTime();
        while (waitNs > 0 && sync.peek() == null) {
            LockSupport.parkNanos(this, waitNs);
            waitNs = CLEAN_INTERVAL_NS - (Timestamp.nanoTime()-waitStart);
        }
    }
}
