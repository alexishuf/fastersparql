package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static com.github.alexishuf.fastersparql.batch.Timestamp.nanoTime;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.concurrent.locks.LockSupport.*;

public class SPSCBIt<B extends Batch<B>> extends AbstractBIt<B> implements CallbackBIt<B> {
    private static final VarHandle READY;
    static {
        try {
            READY = lookup().findVarHandle(SPSCBIt.class, "plainReady", Batch.class);
        } catch (Throwable t) {
            throw new ExceptionInInitializerError(t);
        }
    }

    //private final DebugJournal.RoleJournal dbg;

    @SuppressWarnings("unused")  // access through READY
    private @Nullable B plainReady;
    private @Nullable B filling;
    private long fillingStart = Timestamp.ORIGIN;
    protected int maxItems;
    private Thread consumer, producer;

    public SPSCBIt(BatchType<B> batchType, Vars vars, int maxItems) {
        super(batchType, vars);
        this.maxItems = maxItems;
        //this.dbg = DebugJournal.SHARED.role(toStringNoArgs());
    }

    /* --- --- --- properties --- --- --- */

    @SuppressWarnings("unused") @Override public int                  maxReadyItems()      { return maxItems; }
    @Override public @This CallbackBIt<B> maxReadyItems(int n) { maxItems = n; return this; }

    @Override public boolean isTerminated() {
        return state().isTerminated();
    }

    @Override public boolean isComplete() {
        return state() == State.COMPLETED;
    }

    @Override public boolean isCancelled() {
        return state() == State.CANCELLED;
    }

    @Override public @Nullable Throwable error() {
        if (state() != State.FAILED) return null;
        return error == null ? new RuntimeException("unknown error") :  error;
    }

    /* --- --- --- helper methods --- --- --- */

    /**
     * Either take a non-null batch from {@code READY} (setting it to {@code null}) or
     * acquire the {@code LOCK}.
     * @return {@code null} iff locked, else the non-null batch taken from {@code READY}.
     */
    private B lockOrTakeReady(Thread me) {
        B b;
        //noinspection unchecked
        while ((b=(B)READY.getAndSetAcquire(this, null)) == null && !tryLock(me))
            Thread.yield();
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

    @Override public boolean complete(@Nullable Throwable error) {
        return onTermination(error);
    }

    @Override public boolean cancel(boolean ack) {
        FSException e = ack ? new FSCancelledException()
                            : new FSServerException("server spontaneously cancelled");
        return complete(e);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        try {
            //dbg.write("cleanup: unpark producer=", producer == null ? 0 : 1, "consumer=", consumer == null ? 0 : 1);
            if (filling != null) {
                if (READY.getOpaque(this) == null) {
                    if (filling.rows > 0) READY.setRelease(this, filling);
                    else batchType.recycle(filling);
                    filling = null;
                } else if (filling.rows == 0) {
                    batchType.recycle(filling);
                    filling = null;
                }
            }
            super.cleanup(cause);
        } finally {
            unpark(producer);
            unpark(consumer);
        }
    }

    /* --- --- --- producer methods --- --- --- */

    @Override public B fillingBatch() {
        lock();
        try {
            B f = filling;
            if (f == null) f = batchType.create(vars.size());
            else           filling = null;
            return f;
        } finally {
            unlock();
        }
    }

    @Override public @Nullable B offer(B b) throws TerminatedException, CancelledException {
        if (ThreadJournal.ENABLED)
            journal("offer rows=", b.totalRows(), "on", this);
        var me = Thread.currentThread();
        lock(me);
        Thread delayedWake = null;
        boolean locked = true;
        try {
            while (true) {
                B f = this.filling;
                if (plainState.isTerminated()) {
                    if (plainState == State.CANCELLED)
                        throw CancelledException.INSTANCE;
                    else
                        throw TerminatedException.INSTANCE;
                } else if (f == null) { // no filling batch
                    if (needsStartTime && fillingStart == Timestamp.ORIGIN) fillingStart = nanoTime();
                    if (READY.getOpaque(this) == null && readyInNanos(b.totalRows(), fillingStart) == 0) {
                        READY.setRelease(this, b);
                        fillingStart = Timestamp.ORIGIN;
                        unpark(consumer);
                    } else { // start filling
                        this.filling = b;
                        delayedWake = consumer;
                    }
                    b = null;
                    break;
                } else {
                    boolean park = mustPark(b.totalRows(), f.totalRows());
                    if (plainReady == null && (park || readyInNanos(f.totalRows(), fillingStart) == 0)) {
                        READY.setRelease(this, f);
                        fillingStart = Timestamp.ORIGIN;
                        unpark(consumer);
                        this.filling = b;
                        b = null;
                        break;
                    } else if (park) {
                        producer = me;
                        //dbg.write("offer: parking, b.rows=", b.rows);
                        unlock();
                        locked = false;
                        park(this);
                        lock();
                        producer = null;
                        locked = true;
                    } else {
                        f.append(b);
                        b = null;
                        delayedWake = consumer; // delay unpark() to avoid Thread.yield
                        break;
                    }
                }
            }
        } finally {
            if (locked) unlock();
            if (eager) eager = false;
            unpark(delayedWake);
        }
        return b;
    }

    @Override public void copy(B b) throws TerminatedException, CancelledException {
        if (ThreadJournal.ENABLED)
            journal("copy rows=", b.totalRows(), "on", this);
        var me = Thread.currentThread();
        lock(me);
        boolean locked = true;
        Thread delayedWake = null;
        try {
            if (plainState.isTerminated()) {
                if (plainState == State.CANCELLED)
                    throw CancelledException.INSTANCE;
                throw TerminatedException.INSTANCE;
            }
            while (true) {
                B dst = this.filling;
                // try publishing filling as READY since put() might take > 1us
                if (dst != null && READY.getOpaque(this) == null
                        && readyInNanos(dst.totalRows(), fillingStart) == 0) {
                    READY.setRelease(this, dst);
                    fillingStart = Timestamp.ORIGIN;
                    unpark(consumer); // delayedWake unnecessary
                    dst = null;
                }
                if (dst == null) { // no filling or published filling to READY
                    filling = dst = batchType.create(nColumns);
                    if (needsStartTime && fillingStart == Timestamp.ORIGIN) fillingStart = nanoTime();
                }
                if (mustPark(b.totalRows(), dst.totalRows())) { // park() until free capacity
                    producer = me;
                    //dbg.write("copy: parking, b.rows=", b.rows);
                    unlock();
                    locked = false;
                    park(this);
                    lock();
                    producer = null;
                    locked = true;
                } else { // put and return
                    dst.copy(b);
                    if (READY.getOpaque(this) == null && readyInNanos(dst.totalRows(), fillingStart)==0) {
                        READY.setRelease(this, dst);
                        this.filling = null;
                        fillingStart = Timestamp.ORIGIN;
                        //dbg.write("copy: pub after put");
                    }
                    delayedWake = consumer; // delay wake to avoid Thread.yield()
                    break;
                }
            }
        } finally {
            if (locked) unlock();
            if (eager) eager = false;
            unpark(delayedWake);
        }
    }

    /* --- --- --- consumer methods --- --- --- */

    @Override public @Nullable B nextBatch(@Nullable B offer) {
        // always check READY before trying to acquire LOCK, since writers may hold it for > 1us
        Thread me = Thread.currentThread();
        B b = lockOrTakeReady(me);
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
                    if ((parkNs = readyInNanos(filling.totalRows(), fillingStart)) == 0 || plainState.isTerminated()) {
                        if (filling.rows > 0) b = filling;
                        else                  batchType.recycle(filling);
                        this.filling = null;
                        fillingStart = Timestamp.ORIGIN;
                        break;
                    }
                } else if (plainState.isTerminated()) { // also: READY and filling are null
                    break;
                } else {   // start a filling batch using offer
                    parkNs = Long.MAX_VALUE;
                    if (offer != null) {
                        this.filling = offer.clear(nColumns);
                        offer = null;
                    }
                    if (needsStartTime) fillingStart = nanoTime();
                }
                // park until time-based completion or unpark from offer()/copy()/cleanup()
                //dbg.write("nextBatch: parking, parkNs=", parkNs);
                consumer = me;
                unlock();
                locked = false;
                journal("park ns=", parkNs, "on", this);
                if (parkNs == Long.MAX_VALUE)
                    park(this);
                else
                    parkNanos(this, parkNs);
                if ((b = lockOrTakeReady(me)) != null)
                    break;
                consumer = null;
                locked = true;
            }
        } finally {
            if (locked) unlock();
            // recycle offer if we did not already
            Batch.recycle(offer);
            unpark(producer);
        }

        if (b == null)   // terminal batch
            return onTerminal(); // throw if failed
        onNextBatch(b); // guides getBatch() allocations
        //dbg.write("nextBatch RET &b=", System.identityHashCode(b), "rows=", b.rows);
        return b;
    }

    @SuppressWarnings("SameReturnValue") private @Nullable B onTerminal() {
        lock();
        try {
            filling = Batch.recycle(filling);
        } finally { unlock(); }
        checkError();
        return null;
    }
}
