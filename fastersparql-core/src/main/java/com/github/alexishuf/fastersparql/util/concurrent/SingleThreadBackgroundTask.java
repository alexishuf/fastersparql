package com.github.alexishuf.fastersparql.util.concurrent;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MessagePassingQueue.Consumer;
import org.jctools.queues.atomic.MpscUnboundedAtomicArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

public abstract class SingleThreadBackgroundTask<T, Q extends MessagePassingQueue<T>> extends Thread
        implements BackgroundTask {
    private static final Logger log = LoggerFactory.getLogger(SingleThreadBackgroundTask.class);
    protected static final int PREFERED_QUEUE_CHUNK = 2048/4;
    private static final VarHandle PARKED;
    static {
        try {
            PARKED = MethodHandles.lookup().findVarHandle(SingleThreadBackgroundTask.class, "plainParked", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final Q work;
    private final MpscUnboundedAtomicArrayQueue<Object> sync;
    @SuppressWarnings("unused") private int plainParked;

    public SingleThreadBackgroundTask(String name, Q work) {
        this(name, work, NORM_PRIORITY);
    }

    public SingleThreadBackgroundTask(String name, Q work, int priority) {
        super(name);
        this.work = work;
        sync = new MpscUnboundedAtomicArrayQueue<>(Runtime.getRuntime().availableProcessors()*4);
        setDaemon(true);
        setPriority(priority);
        BackgroundTasks.register(this);
        start();
    }

    protected abstract void handle(T work);

    public void sched(T item) {
        if (item == null)
            throw new NullPointerException("Cannot sched(null)");
        while (!work.offer(item))
            Thread.yield(); // queue is unbounded, should never run
        if ((int)PARKED.compareAndExchangeAcquire(this, 1, 0) == 1)
            Unparker.unpark(this);
    }

    @Override public void sync(CountDownLatch latch) {
        if (sync.offer(latch)) {
            PARKED.setRelease(this, 0);
            Unparker.unpark(this);
        } else {
            assert false : "queue.offer() == false on unbounded queue";
            latch.countDown();
        }
    }

    @Override public final void run() {
        if (Thread.currentThread() != this)
            throw new IllegalStateException("wrong thread");
        boolean parking = false;
        //noinspection InfiniteLoopStatement
        while (true) {
            T item = work.poll();
            if (item == null && sync.drain(ANSWER_SYNC) == 0) {
                parking     = true;
                if ((int)PARKED.getAndAddRelease(this, 1) == 1) {
                    LockSupport.park();
                    parking = false;
                    PARKED.setRelease(this, 0);
                }
            } else {
                if (parking) {
                    parking = false;
                    PARKED.setRelease(this, 0);
                }
                if (item != null) {
                    try {
                        handle(item);
                    } catch (Throwable t) {
                        log.error("{} during {}.handle({})",
                                t.getClass().getSimpleName(), getName(), item, t);
                    }
                }
            }
        }
    }

    static final AnswerSync ANSWER_SYNC = new AnswerSync();
    static final class AnswerSync implements Consumer<Object> {
        @Override public void accept(Object o) {
            try {
                if      (o instanceof Thread         t) Unparker.unpark(t);
                else if (o instanceof CountDownLatch l) l.countDown();
                else                                    log.error("Unexpected sync object: {}", o);
            } catch (Throwable t) {
                log.error("{} while notifying sync object {}", t.getClass().getSimpleName(), o, t);
            }
        }
        @Override public String toString() {return "SingleThreadBackgroundTask.AnswerSync";}
    }
}
