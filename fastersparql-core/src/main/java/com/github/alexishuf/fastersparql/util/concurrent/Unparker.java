package com.github.alexishuf.fastersparql.util.concurrent;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.atomic.MpmcAtomicArrayQueue;

import java.util.concurrent.locks.LockSupport;

public class Unparker {
    private static final int QUEUE_CAPACITY = 32;
    private static final int TICK_UNPARK_LIMIT = QUEUE_CAPACITY>>1;
    private static final MpmcAtomicArrayQueue<Thread> unparkQueue = new MpmcAtomicArrayQueue<>(QUEUE_CAPACITY);
    private static final Thread unparkerThread = new Thread(new UnparkerTask(), "Unparker");
    static {
        unparkerThread.setDaemon(true);
        unparkerThread.setPriority(Thread.NORM_PRIORITY-1);
        unparkerThread.start();
        Timestamp.ON_TICK = new DrainOnTick();
    }

    private static final Unpark UNPARK = new Unpark();
    private static final class Unpark implements MessagePassingQueue.Consumer<Thread> {
        @Override public void accept(Thread t) {LockSupport.unpark(t);}
        @Override public String toString() {return "LockSupport.unpark()";}
    }
    private static final class UnparkerTask implements Runnable {
        @SuppressWarnings("InfiniteLoopStatement") @Override public void run() {
            while (true) {
                unparkQueue.drain(UNPARK);
                LockSupport.park();
            }
        }
    }
    private static final class DrainOnTick implements Runnable {
        private final Thread[] tmp = new Thread[QUEUE_CAPACITY+(128/4)];
        @Override public void run() {
            int n = 0;
            Thread last = null;
            for (Thread t; n < TICK_UNPARK_LIMIT && (t=unparkQueue.relaxedPoll()) != null; ) {
                if (t != last)
                    tmp[n++] = last = t;
            }
            if (n == TICK_UNPARK_LIMIT)
                LockSupport.unpark(unparkerThread);
            for (int i = 0; i < n; i++)
                LockSupport.unpark(tmp[i]);
        }
        @Override public String toString() {return "DrainUnparkQueue";}
    }

    public static boolean volunteer() {
        Thread thread = unparkQueue.poll();
        if (thread == null)
            return false;
        LockSupport.unpark(thread);
        return true;
    }

    public static void unpark(Thread thread) {
        if (thread == null)
            return; // nop
        if (!unparkQueue.offer(thread))
            LockSupport.unpark(thread);
    }
}
