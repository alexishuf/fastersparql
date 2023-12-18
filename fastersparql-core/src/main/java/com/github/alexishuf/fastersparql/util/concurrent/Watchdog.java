package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import static java.lang.invoke.MethodHandles.lookup;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Watchdog implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Watchdog.class);
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
    private volatile boolean triggered, shutdown;
    private final Semaphore semaphore = new Semaphore(0);
    private @MonotonicNonNull ConcurrentLinkedQueue<Runnable> lateActions = null;
    private final Thread thread;

    public Watchdog(Runnable action) {
        this.thread = Thread.startVirtualThread(() -> {
            semaphore.acquireUninterruptibly(); // wait until first start() call
            while (!shutdown) {
                long now = Timestamp.nanoTime(), deadline = (long)DEADLINE.getAcquire(this);
                if (now >= deadline) {
                    if (shutdown) { // re-check for racing close()
                        break;
                    } else if (!triggered) {
                        triggered = true; // only run action at most once per start() call
                        action.run();
                        if (lateActions != null) {
                            for (Runnable a : lateActions)
                                a.run();
                        }
                    }
                    if (!shutdown)
                        semaphore.acquireUninterruptibly(); //wake on start() or close()
                } else {
                    long delta = deadline - now;
                    try {
                        //noinspection ResultOfMethodCallIgnored
                        semaphore.tryAcquire(delta, NANOSECONDS); // wake at deadline/start()/close()
                    } catch (InterruptedException ignored) {}
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
        semaphore.release();
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
        semaphore.release();
        try {
            if (!thread.join(Duration.ofSeconds(5))) {
                StringBuilder sb = new StringBuilder();
                for (StackTraceElement e : thread.getStackTrace())
                    sb.append("\n\tat ").append(e);
                log.warn("Watchdog thread blocked at{}", sb);
            }
        } catch (InterruptedException e) {
            log.error("Interrupted, will not join thread", e);
        }
    }
}
