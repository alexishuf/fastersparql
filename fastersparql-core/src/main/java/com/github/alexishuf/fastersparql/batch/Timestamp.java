package com.github.alexishuf.fastersparql.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.ofPlatform;
import static java.lang.invoke.MethodHandles.lookup;

public class Timestamp {
    private static final Logger log = LoggerFactory.getLogger(Timestamp.class);
    private static final VarHandle NOW, ERR;
    static {
        try {
            NOW = lookup().findStaticVarHandle(Timestamp.class, "plainNow", long.class);
            ERR = lookup().findStaticVarHandle(Timestamp.class, "plainErr", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private static long plainNow;
    @SuppressWarnings("unused") private static long plainErr;
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
     * {@link System#nanoTime()}). See {@link #nanoTimeError()} for the observed maximum error in
     * in a recent window.</p>
     *
     * @return A (very) loose approximation for the number of nanoseconds since the Timestamp
     *         class was loaded (which implicitly happens before the first call to any of its
     *         public methods).
     */
    public static long nanoTime() { return (long)NOW.getOpaque(); }

    /**
     * Get a weighted moving average of the maximum error observed between {@link #nanoTime()}
     * and {@link System#nanoTime()}.
     */
    public static long nanoTimeError() { return (long)ERR.getOpaque(); }

    /* --- --- --- internal --- --- --- */

    private static void tick() {
        //noinspection InfiniteLoopStatement
        while (true) {
            // Linux and Windows impose their own minimum bounds on wait times and will let
            // this thread sleeping more than it requested.
            LockSupport.parkNanos(50_000);
            long now = System.nanoTime();
            long elapsed = now-(long)NOW.getAndSet(now);

            // err is the moving average between elapsed and the last 15 elapsed values
            ERR.setOpaque((15*(long)ERR.getOpaque() + elapsed)>>4);

            if (Thread.interrupted() && !tickerInterruptLogged) {
                tickerInterruptLogged = true;
                log.warn("Ignoring thread interrupt. Will silently ignore future interrupts.");
            }
        }
    }
}
