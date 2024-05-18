package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntFunction;

import static com.github.alexishuf.fastersparql.util.concurrent.SingleThreadBackgroundTask.ANSWER_SYNC;

public abstract class LevelCleanerBackgroundTask<T> extends Thread implements BackgroundTask {
    private static final Logger log = LoggerFactory.getLogger(LevelCleanerBackgroundTask.class);
    private static final VarHandle PARKED;
    static {
        try {
            PARKED = MethodHandles.lookup().findVarHandle(LevelCleanerBackgroundTask.class, "plainParked", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private final MpmcUnboundedXaddArrayQueue<T>[] queues;
    private final MpscUnboundedAtomicArrayQueue<Object> sync;
    @SuppressWarnings("unused") private int plainParked;
    public @MonotonicNonNull LevelAlloc<T> pool;
    public final ClearElseMake clearElseMake;
    public final IntFunction<T> newInstance;
    protected int objsPooled;
    protected int fullPoolGarbage;

    @SuppressWarnings("unchecked")
    public LevelCleanerBackgroundTask(String name, IntFunction<T> supplier) {
        super(name);
        int threads     = Runtime.getRuntime().availableProcessors();
        newInstance     = supplier;
        clearElseMake = new ClearElseMake(supplier);
        sync            = new MpscUnboundedAtomicArrayQueue<>(threads*4);
        queues          = new MpmcUnboundedXaddArrayQueue[33];
        for (int i = 0; i < queues.length; i++)
            queues[i] = new MpmcUnboundedXaddArrayQueue<>(2048/4);
        setDaemon(true);
        setPriority(NORM_PRIORITY);
        start();
    }

    public final class ClearElseMake implements IntFunction<T> {
        private final IntFunction<T> supplier;

        public ClearElseMake(IntFunction<T> supplier) {
            this.supplier = supplier;
        }

        @Override public T apply(int len) {
            int level = LevelAlloc.len2level(len);
            LevelAlloc<T> pool = LevelCleanerBackgroundTask.this.pool;
            if (pool == null || pool.isLevelPooled(level)) {
                var queue = queues[level];
                T o = queue.relaxedPoll();
                if (o == null) {
                    onSpinWait();
                    o = queue.relaxedPoll();
                }
                if (o != null) {
                    clear(o);
                    return o;
                }
            }
            return supplier.apply(len);
        }
    }


    @Override public void sync(CountDownLatch latch) {
        if (sync.offer(latch)) {
            Unparker.unpark(this);
        } else {
            latch.countDown();
            assert false : "offer() == false on unbounded queue";
        }
    }

    public void sched(T obj, int len) { schedAtLevel(obj, LevelAlloc.len2level(len)); }

    public void schedAtLevel(T obj, int level) {
        if (pool == null || pool.isLevelPooled(level)) {
            while (!queues[level].offer(obj))
                Thread.yield(); // offer() always returns true as queue is unbounded
            if ((int)PARKED.compareAndExchangeRelease(this, 1, 0) == 1)
                Unparker.unpark(this);
        }
    }

    protected abstract void clear(T obj);

    @Override public void run() {
        if (Thread.currentThread() != this)
            throw new IllegalStateException("run() called from unexpected thread");
        boolean parking = false;
        //noinspection InfiniteLoopStatement
        while (true) {
            boolean empty = true;
            for (int i = 0; i < queues.length; i++) {
                T o = queues[i].relaxedPoll();
                if (o != null) {
                    empty = false;
                    try {
                        clear(o);
                        if (pool.offerToLevelShared(o, i) == null)
                            ++objsPooled;
                        else
                            ++fullPoolGarbage;
                    } catch (Throwable t) {
                        log.error("{} during clear/pool of {}: ",
                                  t.getClass().getSimpleName(), o, t);
                    }
                }
            }
            if (empty && sync.drain(ANSWER_SYNC) == 0) {
                parking = true;
                if ((int) PARKED.getAndAddRelease(this, 1) == 1) {
                    LockSupport.park();
                    PARKED.setRelease(this, 0);
                }
            } else if (parking) {
                parking = false;
                PARKED.setRelease(this, 0);
            }
        }
    }
}
