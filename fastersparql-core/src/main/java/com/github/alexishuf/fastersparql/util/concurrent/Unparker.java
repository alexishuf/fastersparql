package com.github.alexishuf.fastersparql.util.concurrent;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.atomic.MpmcAtomicArrayQueue;

import java.util.concurrent.locks.LockSupport;

public class Unparker {
    private static final MpmcAtomicArrayQueue<Thread> unparkQueue = new MpmcAtomicArrayQueue<>(16);
    private static final Thread thread = new Thread(new UnparkerTask(), "Unparker");
    static {
        thread.setDaemon(true);
        thread.setPriority(Thread.NORM_PRIORITY-1);
        thread.start();
        Timestamp.ON_TICK = new UnparkUnparker();
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
                LockSupport.parkNanos(61_000);
            }
        }
    }
    private static final class UnparkUnparker implements Runnable {
        @Override public void run() {
            if (!unparkQueue.isEmpty())
                LockSupport.unpark(thread);
        }
        @Override public String toString() {return "UnparkUnparker";}
    }

    public static void unpark(Thread thread) {
        if (!unparkQueue.offer(thread))
            LockSupport.unpark(thread);

    }
}
