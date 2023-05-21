package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.batch.Timestamp.nanoTime;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.concurrent.locks.LockSupport.*;

public class SPSCBIt<B extends Batch<B>> extends AbstractBIt<B> implements CallbackBIt<B> {
    private static final VarHandle LOCK;
    private static final VarHandle READY;
    static {
        try {
            LOCK = lookup().findVarHandle(SPSCBIt.class, "plainLock", int.class);
            READY = lookup().findVarHandle(SPSCBIt.class, "plainReady", Batch.class);
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }

    //private final DebugJournal.RoleJournal dbg;

    @SuppressWarnings("unused")  // access through READY
    private @Nullable B plainReady;
    private @Nullable B filling;
    private long fillingStart = ORIGIN;
    protected int maxItems;
    @SuppressWarnings("unused") // access through STATE
    private int plainLock;
    private Thread consumer, producer;

    public SPSCBIt(BatchType<B> batchType, Vars vars, int maxItems) {
        super(batchType, vars);
        this.maxItems = maxItems;
        //this.dbg = DebugJournal.SHARED.role(toStringNoArgs());
    }

    /* --- --- --- properties --- --- --- */

    @Override public int                maxReadyBatches()      { return 2; }
    @Override public int                  maxReadyItems()      { return maxItems; }
    @Override public @This CallbackBIt<B> maxReadyItems(int n) { maxItems = n; return this; }

    @Override public boolean isCompleted() {
        lock();
        try {
            return terminated;
        } finally { LOCK.setRelease(this, 0); }
    }

    @Override public boolean isFailed() {
        lock();
        try {
            return error != null;
        } finally { LOCK.setRelease(this, 0); }
    }
    /* --- --- --- helper methods --- --- --- */

    /**
     * Spin until {@code LOCK} is exclusively held by the caller thread.
     *
     * <p>Locking is not recursive. Unlock with {@code LOCK.setRelease(this, 0)}.</p>
     */
    private void lock() {
        while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.yield();
    }

    /**
     * Either take a non-null batch from {@code READY} (setting it to {@code null}) or
     * acquire the {@code LOCK}.
     * @return {@code null} iff locked, else the non-null batch taken from {@code READY}.
     */
    private B lockOrTakeReady() {
        B b;
        while (true) {
            //noinspection unchecked
            b = (B)READY.getAndSetAcquire(this, null);
            if (b != null || (int)LOCK.compareAndExchangeAcquire(this, 0, 1) == 0)
                break;
            Thread.yield();
        }
        return b;
    }

    /***
     * Whether {@link #offer(Batch)} or {@link #copy(Batch)} should {@link LockSupport#park()}
     * when adding {@code offerRows} rows if this queue already has {@code queuedRows} rows queued.
     *
     * <p>The default implementation enforces the {@link CallbackBIt#maxReadyItems(int)}
     * contract. Subclasses may override to implement alternative backpressure modes if blocking
     * is not allowed in their thread (e.g. netty).</p>
     *
     * @param offerRows rows that are being offered via {@link #offer(Batch)} or {@link #copy(Batch)}
     * @param queuedRows number of rows already queued, waiting for a {@link #nextBatch(Batch)} call
     * @return {@code true} iff the {@link #offer(Batch)}/{@link #copy(Batch)} thread should
     *         park until {@link #nextBatch(Batch)} takes some rows.
     */
    protected boolean mustPark(int offerRows, int queuedRows) {
        return offerRows+queuedRows > maxItems && queuedRows > 0;
    }

    /* --- --- --- termination methods --- --- --- */

    @Override public void complete(@Nullable Throwable error) {
        lock();
        try {
            onTermination(error);
        } finally { LOCK.setRelease(this, 0); }
        unpark(producer);
        unpark(consumer);
    }

    @Override public void close() {
        lock();
        try {
            super.close();
        } finally { LOCK.setRelease(this, 0); }
        unpark(producer);
        unpark(consumer);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        //dbg.write("cleanup: unpark producer=", producer == null ? 0 : 1, "consumer=", consumer == null ? 0 : 1);
        if (READY.getOpaque(this) == null && filling != null) {
            if   (filling.rows > 0) READY.setRelease(this, filling);
            else                    batchType.recycle(filling);
            filling = null;
        }
        super.cleanup(cause);
    }

    /* --- --- --- producer methods --- --- --- */

    @Override public @Nullable B offer(B b) throws BItCompletedException {
        lock();
        Thread delayedWake = null;
        boolean locked = true;
        try {
            while (true) {
                B f = this.filling;
                if (terminated) {
                    throw mkCompleted();
                } else if (f == null) { // no filling batch
                    if (needsStartTime && fillingStart == ORIGIN) fillingStart = nanoTime();
                    if (READY.getOpaque(this) == null && readyInNanos(b.rows, fillingStart) == 0) {
                        READY.setRelease(this, b);
                        fillingStart = ORIGIN;
                        unpark(consumer);
                    } else { // start filling
                        this.filling = b;
                        delayedWake = consumer;
                    }
                    b = null;
                    break;
                } else if (READY.getOpaque(this) == null && readyInNanos(f.rows, fillingStart) == 0) {
                    READY.setRelease(this, f);
                    fillingStart = ORIGIN;
                    unpark(consumer);
                    this.filling = b;
                    b = null;
                    break;
                } else if (mustPark(b.rows, f.rows)) {
                    producer = Thread.currentThread();
                    //dbg.write("offer: parking, b.rows=", b.rows);
                    LOCK.setRelease(this, 0);
                    locked = false;
                    park(this);
                    producer = null;
                    lock();
                    locked = true;
                } else {
                    f.put(b);
                    delayedWake = consumer; // delay to avoid Thread.yield
                    break;
                }
            }
        } finally {
            if (locked) LOCK.setRelease(this, 0);
            unpark(delayedWake);
        }
        return b == null ? stealRecycled() : b;
    }

    @Override public void copy(B b) throws BItCompletedException {
        lock();
        boolean locked = true;
        Thread delayedWake = null;
        try {
            if (terminated) throw mkCompleted();
            while (true) {
                B dst = this.filling;
                // try publishing filling as READY since put() might take > 1us
                if (dst != null && READY.getOpaque(this) == null
                        && readyInNanos(dst.rows, fillingStart) == 0) {
                    READY.setRelease(this, dst);
                    fillingStart = ORIGIN;
                    unpark(consumer); // delayedWake unnecessary
                    dst = null;
                }
                if (dst == null) { // no filling or published filling to READY
                    filling = dst = getBatch(null);
                    if (needsStartTime && fillingStart == ORIGIN) fillingStart = nanoTime();
                }
                if (mustPark(b.rows, dst.rows)) { // park() until free capacity
                    producer = Thread.currentThread();
                    //dbg.write("copy: parking, b.rows=", b.rows);
                    LOCK.setRelease(this, 0);
                    locked = false;
                    park(this);
                    producer = null;
                    lock();
                    locked = true;
                } else { // put and return
                    dst.put(b);
                    if (READY.getOpaque(this) == null && readyInNanos(dst.rows, fillingStart)==0) {
                        READY.setRelease(this, dst);
                        this.filling = null;
                        fillingStart = ORIGIN;
                        //dbg.write("copy: pub after put");
                    }
                    delayedWake = consumer; // delay wake to avoid Thread.yield()
                    break;
                }
            }
        } finally {
            if (locked)  LOCK.setRelease(this, 0);
            unpark(delayedWake);
        }
    }

    /* --- --- --- consumer methods --- --- --- */

    @Override public @Nullable B nextBatch(@Nullable B offer) {
        offer = recycle(offer);
        // always check READY before trying to acquire LOCK, since writers may hold it for > 1us
        B b = lockOrTakeReady();
        boolean locked = b == null;
        try {
            if (!locked) return b; // fast path
            long parkNs;
            while (true) {
                B filling = this.filling;
                //noinspection unchecked
                if ((b = (B)READY.getAndSetAcquire(this, null)) != null) {
                    break;
                } else if (filling != null) { // steal or determine nanos until re-check
                    if ((parkNs = readyInNanos(filling.rows, fillingStart)) == 0 || terminated) {
                        if (filling.rows > 0) b = filling;
                        else                  batchType.recycle(filling);
                        this.filling = null;
                        fillingStart = ORIGIN;
                        break;
                    }
                } else if (terminated) { // also: READY and filling are null
                    break;
                } else {   // start a filling batch using offer
                    parkNs = Long.MAX_VALUE;
                    if (offer != null) {
                        offer.clear(vars.size());
                        this.filling = offer;
                        offer = null;
                    }
                    if (needsStartTime) fillingStart = nanoTime();
                }
                // park until time-based completion or unpark from offer()/copy()/cleanup()
                //dbg.write("nextBatch: parking, parkNs=", parkNs);
                consumer = Thread.currentThread();
                LOCK.setRelease(this, 0);
                locked = false;
                parkNanos(this, parkNs);
                consumer = null;
                if ((b = lockOrTakeReady()) != null) {
                    break;
                }
                locked = true;
            }
        } finally {
            if (locked) LOCK.setRelease(this, 0);
            // recycle offer if we did not already
            if (offer != null && recycle(offer) != null) batchType.recycle(offer);
            unpark(producer);
        }

        if (b == null) {  // terminal batch
            checkError(); // throw if failed
            return null;
        }
        onBatch(b); // guides getBatch() allocations
        //dbg.write("nextBatch RET &b=", System.identityHashCode(b), "rows=", b.rows);
        return b;
    }
}
