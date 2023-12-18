package com.github.alexishuf.fastersparql.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

public class Unparker {
    private static final VarHandle LOCK;
    static {
        try {
            LOCK  = MethodHandles.lookup().findStaticVarHandle(Unparker.class, "plainLock",  int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final long PERIOD_NS = 10_000L;
    private static final int CAPACITY = 256/4;
    private static final int UNPARK_AT_CAPACITY = Math.max(CAPACITY/4, 8);
    private static final int CAPACITY_MASK = CAPACITY-1;
    static { assert Integer.bitCount(CAPACITY) == 1; }
    private static final Thread[] queue = new Thread[CAPACITY];
    @SuppressWarnings("unused") private static int plainLock;
    private static int size, takeIdx;
    private static final Thread thread;

    static {
        thread = new Thread(Unparker::unparkQueued, "Unparker");
        thread.setDaemon(true);
        thread.setPriority(Math.max(Thread.MIN_PRIORITY, Thread.NORM_PRIORITY-1));
        thread.start();
    }


    private static void unparkQueued() {
        while (true) {
            while (true) {
                while (!tryLock()) Thread.yield();
                Thread thread = null;
                if (size > 0) {
                    thread = queue[takeIdx];
                    takeIdx = (takeIdx+1)&CAPACITY_MASK;
                    --size;
                }
                LOCK.setRelease(0);
                if (thread == null) break;
                else                 LockSupport.unpark(thread);
            }
            LockSupport.parkNanos(PERIOD_NS);
        }
    }

    private static boolean tryLock() {
        for (int i = 0; i < 16; i++) {
            if ((int)LOCK.compareAndExchangeAcquire(0, 1) == 0) return true;
            else                                                Thread.onSpinWait();
        }
        return false;
    }

    public static void unpark(Thread thread) {
        boolean unpark = true;
        if (tryLock()) {
            if (size < CAPACITY) {
                unpark = false;
                queue[(takeIdx+size)&CAPACITY_MASK] = thread;
                ++size;
            }
            LOCK.setRelease(0);
            if (size == UNPARK_AT_CAPACITY)
                LockSupport.unpark(Unparker.thread);
        }
        if (unpark)
            LockSupport.unpark(thread);
    }

}
