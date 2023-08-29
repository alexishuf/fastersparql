package com.github.alexishuf.fastersparql.batch;

import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.ofPlatform;
import static java.lang.invoke.MethodHandles.lookup;

public class Timestamp {
    /** A value smaller than any {@link #nanoTime()} call without overflow risks. */
    public static final long ORIGIN = System.nanoTime();
    private static final Logger log = LoggerFactory.getLogger(Timestamp.class);
    private static final long PERIOD_NS = 50_000;
    private static final VarHandle NOW;

    static {
        try {
            NOW = lookup().findStaticVarHandle(Timestamp.class, "plainNow", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings({"unused", "FieldMayBeFinal"}) private static long plainNow = System.nanoTime();
    private static long delta = PERIOD_NS;
    private static boolean tickerInterruptLogged = false;
    @SuppressWarnings("unused") private static final Thread ticker
            = ofPlatform().name("Timestamp-tick").daemon(true).start(Timestamp::tick);

    /**
     * Get a lower bound on the number of nanoseconds elapsed since an unspecified fixed
     * point in the past specific to this JVM instance.
     *
     * <p>This method is a {@link System#nanoTime()} replacement that trades precision of the
     * result for latency introduced by the measurement. {@link System#nanoTime()} only is
     * low-latency if it does not trigger a syscall. However, on some hardware, the underlying
     * {@code clock_gettime()} call may trigger a syscall if the kernel deemed the result of the
     * {@code rdtsc} x86 instruction to bee too unreliable to be fixed in software. The result
     * from {@code rdtsc} is fundamentally unreliable on multicore systems since each core keeps
     * a counter that may be out of sync to the other cores and may independently change its
     * increment rate. Such lack of cross-core synchornization causes {@code rdtsc} to be
     * non-monotonic in practice since threads are rarely pinned to a specific core.</p>
     *
     *
     * <p>The result value will always be behind the value reported by a direct
     * {@link System#nanoTime()} call (it can be seen as a lowe bound for
     * {@link System#nanoTime()}).</p>
     *
     * @return A (very) loose approximation for the number of nanoseconds since the Timestamp
     *         class was loaded (which implicitly happens before the first call to any of its
     *         public methods).
     */
    public static long nanoTime() { return (long)NOW.getOpaque(); }

    /**
     * {@link #nanoTime()}+{@link #tickDelta()}
     */
    public static long nextTick() { return (long)NOW.getOpaque()+delta; }

    /**
     * Equivalent to {@link #nanoTime()}{@code + ticks*}{@link #tickDelta()}
     * @param ticks How many ticks in the future. {@code 1} makes the result the same as
     *              {@link #nextTick()}
     * @return The estimated value of the {@code ticks}-th {@link #nanoTime()} value
     *         after the current one.
     */
    public static long nextTick(@NonNegative int ticks) {
        return (long)NOW.getOpaque() + ticks*delta;
    }

    /**
     * Length of the current tick: the next lowest possible value to be observed in
     * {@link #nanoTime()} will be the current value plus the delta reported by this method.
     *
     * <p>This delta is correlated with the wall-clock time delta in nanoseconds between the
     * precise instants when {@link #nanoTime()} got its current value and when it will receive
     * its next value. However, <strong>both the delta value and its deviation from wall-clock
     * time</strong> change frequently. The deviation from wall-clock time suffers from
     * unpredictable jitter. The delta itself changes periodically, but the periods of change
     * are affected by the wall-clock deviation jitter.</p>
     *
     * @return the expected difference between the current and the next {@link #nanoTime()} values.
     */
    @SuppressWarnings("unused") public static long tickDelta() { return delta; }

    /* --- --- --- internal --- --- --- */

    private static void tick() {
        long delta = initDelta();
        //noinspection InfiniteLoopStatement
        for (int i = 0; true; ++i) {
            if ((i & 511) == 0) {
                long before = System.nanoTime();
                LockSupport.parkNanos(PERIOD_NS);
                // compute moving average over last 8 samples (including this)
                Timestamp.delta = delta = ((System.nanoTime()-before) + delta)>>1;
            } else {
                LockSupport.parkNanos(PERIOD_NS);
            }
            NOW.getAndAddRelease(delta);
            if (Thread.interrupted() && !tickerInterruptLogged) {
                tickerInterruptLogged = true;
                log.warn("Ignoring thread interrupt. Will silently ignore future interrupts.");
            }
        }
    }

    private static long initDelta() {
        long delta = PERIOD_NS;
        int rounds = (int)(1_000_000/PERIOD_NS);
        for (int i = 0; i < rounds; i++) {
            long before = System.nanoTime();
            LockSupport.parkNanos(PERIOD_NS);
            delta = System.nanoTime()-before;
        }
        Timestamp.delta = delta /= rounds;
        return delta;
    }
}
