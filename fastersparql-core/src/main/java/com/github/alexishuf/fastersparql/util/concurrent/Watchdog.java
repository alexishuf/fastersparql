package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

import static java.lang.invoke.MethodHandles.lookup;

public class Watchdog implements AutoCloseable {
    private static final VarHandle DEADLINE;

    static {
        try {
            DEADLINE = lookup().findVarHandle(Watchdog.class, "deadline", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") // access through DEADLINE
    private long deadline;
    private boolean triggered, shutdown;
    private @MonotonicNonNull ConcurrentLinkedQueue<Runnable> lateActions = null;
    private final Thread thread;

    public Watchdog(Runnable action) {
        this.thread = Thread.startVirtualThread(() -> {
            LockSupport.park(); // wait until first start() call
            while (!shutdown) {
                long now = Timestamp.nanoTime(), deadline = (long) DEADLINE.getAcquire(this);
                if (now >= deadline) {
                    if (shutdown) { // courtesy re-check for racing close()
                        break;
                    } else if (!triggered) {
                        triggered = true; // only run action at most once per start() call
                        action.run();
                        if (lateActions != null) {
                            for (Runnable a : lateActions)
                                a.run();
                        }
                    }
                    LockSupport.park(); //wake on start() or close()
                } else {
                    long delta = deadline - now;
                    LockSupport.parkNanos(delta); // wake at deadline/start()/close()
                }
            }
        });
    }

    @SuppressWarnings("unused") public @This Watchdog andThen(Runnable action) {
        if (lateActions == null) lateActions = new ConcurrentLinkedQueue<>();
        lateActions.add(action);
        return this;
    }

    /**
     * Starts/resets the watchdog so that it will trigger and run the constructor-provided action
     * once {@code nanos} nanoseconds have elapsed since this call. The action will trigger at
     * most once per {@code start()} call (i.e., single-shot as opposed to periodic).
     *
     * <p>Future {@code start()} and {@link #stop()} calls cancel this call, i.e., the
     * watchdog will not trigger the action at the deadline implied by the first call if the
     * second call happens before the deadline is observed by the background thread.</p>
     *
     * @param nanos After how many nanoseconds (counting from this call) the action provided
     *              in the {@link Watchdog} constructor must run (once).
     * @return {@code this}
     * @throws IllegalStateException if {@link #close()} was previously called
     */
    public @This Watchdog start(long nanos) {
        if (shutdown) throw new IllegalStateException("close()ed");
        triggered = false;
        DEADLINE.setRelease(this, Timestamp.nanoTime()+nanos);
        LockSupport.unpark(thread);
        return this;
    }

    /**
     * Cancels a previous {@link #start(long)} call if it has not yet triggered.
     *
     * @return {@code this}
     */
    public @This Watchdog stop() {
        triggered = false;
        DEADLINE.setRelease(this, Long.MAX_VALUE);
        return this;
    }


    /**
     * Cancels the effect of any not-yet-triggered {@link #start(long)} and stops the
     * watchdog thread.
     *
     * <p>After this method is called, calling {@link #start(long)} is not allowed.</p>
     */
    @Override public void close() {
        shutdown = true;
        DEADLINE.setRelease(this, Long.MAX_VALUE);
        LockSupport.unpark(thread);
    }
}
