package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.currentThread;

public abstract class SPSCBIt<T> extends AbstractBIt<T> implements CallbackBIt<T> {
    private final AtomicInteger spinlock = new AtomicInteger();
    private final AtomicReference<Thread> lockWaiter = new AtomicReference<>();
    @Nullable protected Thread signalWaiter;
    private boolean signaled;
    private @Nullable Thread owner;

    public SPSCBIt(RowType<T> rowType, Vars vars) {
        super(rowType, vars);
    }

    protected final void lock() {
        Thread me = currentThread();
        int saw = spinlock.compareAndExchangeAcquire(0, 1);
        if (saw == 0) { // acquired the spinlock
            owner = me;
        } else if (owner == me) { // recursive lock()
            spinlock.setPlain(saw + 1);
        } else { // another thread has the lock, spin
            LockSupport.setCurrentBlocker(this);
            boolean canPark = lockWaiter.weakCompareAndSetAcquire(null, me);
            try {
                for (int i = 0; spinlock.compareAndExchangeAcquire(0, 1) != 0; i++) {
                    Thread.onSpinWait(); // help HyperThreading
                    if ((i & 64) != 0) {
                        if (canPark) LockSupport.park();
                        else Thread.yield();
                    }
                }
                owner = me;
            } finally {
                if (canPark) lockWaiter.setRelease(null);
                LockSupport.setCurrentBlocker(null);
            }
        }
    }

    protected final void unlock() { unlock(spinlock.getPlain() - 1); }

    private void unlock(int depth) {
        if (owner != currentThread() || depth < 0)
            throw new IllegalStateException("mismatched unlock()");
        if (depth == 0)
            owner = null;
        Thread signalWaiter;
        if (depth == 0 && signaled) {
            signalWaiter = this.signalWaiter;
            signaled = false;
        } else {
            signalWaiter = null;
        }
        spinlock.setRelease(depth);
        LockSupport.unpark(signalWaiter);
        LockSupport.unpark(lockWaiter.getAcquire());
    }

    protected final void signal() {
        signaled = true;
    }

    protected final void await() {
        int depth = spinlock.getOpaque();
        Thread me = currentThread();
        signalWaiter = me;
        unlock(0);
        LockSupport.park(this);
        lock();
        if (signalWaiter == me) signalWaiter = null;
        if (depth > 1) spinlock.setOpaque(depth);
    }

    protected final long awaitNanos(long timeoutNanos) {
        int depth = spinlock.getOpaque();
        Thread me = currentThread();
        signalWaiter = me;
        unlock(0);
        // computing nanoTime()-start takes ~1.12us (JMH on "my machine"). parkNanos() will cost
        // at least that much. Therefore, computing remNanos is wasteful for small timeoutNanos
        long start = timeoutNanos < 3_000L ? 0 : System.nanoTime();
        LockSupport.parkNanos(this, timeoutNanos);
        if (start == 0 || (timeoutNanos -= System.nanoTime()-start) < 1_500L)
            timeoutNanos = 0;
        lock();
        if (signalWaiter == me) signalWaiter = null;
        if (depth > 1) spinlock.setOpaque(depth);
        return timeoutNanos;
    }

    @Override public void complete(@Nullable Throwable error) {
        lock();
        try {
            onTermination(error);
        } finally { unlock(); }
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        LockSupport.unpark(signalWaiter);
    }

    /**
     * Poll if this {@link SPSCBufferedBIt#complete(Throwable)} has been previously called.
     *
     * <p> Using this method is not thread-safe. It should be used only if the caller is the
     * single thread responsible for calling {@link SPSCBufferedBIt#complete(Throwable)} or for
     * debugging reasons.</p>
     *
     * @return {@code true} if {@code this.complete(cause)} was previously called.
     */
    public boolean isComplete() {
        return terminated;
    }

    /**
     * Whether {@code this} was {@link SPSCBufferedBIt#complete(Throwable)}ed with a non-null
     * {@code cause}.
     *
     * <p>Like {@link SPSCBufferedBIt#complete(Throwable)}, this should be called by the only
     * thread that calls {@link SPSCBufferedBIt#complete(Throwable)} otr for debugging reasons.</p>
     *
     * @return {@code true} if {@link SPSCBufferedBIt#isComplete()} and the
     *         {@link SPSCBufferedBIt#complete(Throwable)} in effect call used a non-null {@code cause}.
     */
    public boolean isFailed() {
        return terminated && error != null;
    }

}
