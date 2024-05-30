package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BItCancelledException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static com.github.alexishuf.fastersparql.util.concurrent.Timestamp.ORIGIN;
import static com.github.alexishuf.fastersparql.util.concurrent.Timestamp.nanoTime;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.locks.LockSupport.*;

public class SPSCBIt<B extends Batch<B>> extends AbstractBIt<B> implements CallbackBIt<B> {
    private @Nullable B ready;
    private @Nullable B filling;
    private Thread consumer, producer;
    protected long queuedRows;
    protected int maxItems;
    private long fillingStart = Timestamp.ORIGIN;
    private @Nullable StreamNode upstream;

    public SPSCBIt(BatchType<B> batchType, Vars vars) {
        this(batchType, vars, FSProperties.itQueueRows(batchType, vars.size()));
    }
    public SPSCBIt(BatchType<B> batchType, Vars vars, int maxItems) {
        super(batchType, vars);
        this.maxItems = maxItems;
        //this.dbg = DebugJournal.SHARED.role(toStringNoArgs());
    }

    /* --- --- --- properties --- --- --- */

    @SuppressWarnings("unused")
    @Override public int                  maxReadyItems()      { return maxItems; }
    @Override public @This CallbackBIt<B> maxReadyItems(int n) { maxItems = n; return this; }

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
    /* --- --- --- StreamNode --- --- --- */

    public @This SPSCBIt<B> upstream(StreamNode node) {
        upstream = node;
        return this;
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Stream.ofNullable(upstream);
    }

    @Override protected void appendStateToLabel(StringBuilder sb) {
        super.appendStateToLabel(sb);
        sb.append("\nqueuedRows=").append(queuedRows);
    }

    /* --- --- --- helper methods --- --- --- */

    /***
     * Whether {@link #offer(Orphan)} or {@link #copy(Batch)} should {@link LockSupport#park()}
     * when adding {@code offerRows} rows if this queue already has {@code queuedRows} rows queued.
     *
     * <p>The default implementation enforces the {@link CallbackBIt#maxReadyItems(int)}
     * contract. Subclasses may override to implement alternative backpressure modes if blocking
     * is not allowed in their thread (e.g. netty).</p>
     *
     * @param offerRows rows that are being offered via {@link #offer(Orphan)} or {@link #copy(Batch)}
     * @param queuedRows number of rows already queued, waiting for a {@link #nextBatch(Orphan)} call
     * @return {@code true} iff the {@link #offer(Orphan)}/{@link #copy(Batch)} thread should
     *         park until {@link #nextBatch(Orphan)} takes some rows.
     */
    protected boolean mustPark(int offerRows, long queuedRows) {
        return queuedRows > 0 && offerRows+queuedRows > maxItems && notTerminated();
    }

    /* --- --- --- termination methods --- --- --- */

    @Override public boolean complete(@Nullable Throwable error) {
        if (ThreadJournal.ENABLED)
            journal(isTerminated() ? "late complete" : "complete", error, "on", this);
        return onTermination(error);
    }

    @Override public boolean cancel(boolean ack) {
        if (ThreadJournal.ENABLED)
            journal(isTerminated() ? "late cancel ack=" : "cancel, ack=", ack?1:0, "on", this);
        if (ack)
            return complete(new FSCancelledException());
        else
            return tryCancel();

    }

    protected void dropAllQueued() {
        lock();
        ready   = Batch.safeRecycle(ready,   this);
        filling = Batch.safeRecycle(filling, this);
        queuedRows = 0;
        unlock();
        unpark(producer); // may be waiting for free capacity
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        try {
            if (cause instanceof BItCancelledException) { // drop all queued
                ready   = Batch.safeRecycle(ready,   this);
                filling = Batch.safeRecycle(filling, this);
            } else if (filling != null) {
                if (filling.rows == 0)
                    Batch.safeRecycle(filling, this);
                else
                    ready = Batch.quickAppend(ready, this, filling.releaseOwnership(this));
                filling = null;
            }
            super.cleanup(cause);
        } finally {
            unpark(producer);
            unpark(consumer);
        }
    }

    @Override public void close() {
        super.close();
        dropAllQueued();
    }

    /* --- --- --- producer methods --- --- --- */

    @Override public @Nullable Orphan<B> pollFillingBatch() {
        B stale = filling; // returning null is cheaper than synchronization
        if (stale == null || stale.next == null || !tryLock())
            return null; // no queue, no tail or contended
        try {
            return pollFillingBatch0();
        } finally { unlock(); }
    }

    private @Nullable Orphan<B> pollFillingBatch0() {
        B f = filling;
        Orphan<B> tail = null;
        if (f != null) {
            if ((tail = f.detachTail(this)) == f)
                this.filling = null;
            int tailRows = Batch.peekRows(tail);
            queuedRows -= tailRows;
            journal("pollFillingBatch, rows=", tailRows, "on", this);
        }
        return tail;
    }

    @Override public Orphan<B> fillingBatch() {
        lock();
        try {
            Orphan<B> tail = pollFillingBatch();
            return tail == null ? batchType.create(nColumns) : tail;
        } finally { unlock(); }
    }

    protected final boolean   lockAndGet() {   lock(); return  true; }
    protected final boolean unlockAndGet() { unlock(); return false; }

    @Override public void offer(Orphan<B> b) throws TerminatedException, CancelledException {
        Thread delayedWake = null;
        int bRows = Batch.peekTotalRows(b);
        boolean locked = lockAndGet();
        try {
            while (true) {
                if (plainState.isTerminated()) {
                    if (plainState == State.CANCELLED) throw CancelledException.INSTANCE;
                    else                               throw TerminatedException.INSTANCE;
                } else if (queuedRows > 0 && mustPark(bRows, queuedRows)) {
                    producer = currentThread();
                    locked = unlockAndGet();
                    park(this);
                    producer = null;
                    locked = lockAndGet();
                } else if (filling == null) { // no filling batch
                    if (needsStartTime && fillingStart == ORIGIN)
                        fillingStart = nanoTime();
                    if (ready == null && readyInNanos(bRows, fillingStart) == 0) {
                        ready = b.takeOwnership(this);
                        fillingStart = ORIGIN;
                    } else {
                        filling = b.takeOwnership(this);
                    }
                    b = null;
                    queuedRows += bRows;
                    delayedWake = consumer;
                    break;
                } else  { // append b to filling while unlocked
                    Orphan<B> fillingOrphan = pollFillingBatch0();
                    locked = unlockAndGet();
                    if (fillingOrphan == null)
                        fillingOrphan = batchType.create(nColumns);
                    var f = fillingOrphan.takeOwnership(this);
                    f.append(b);
                    bRows = f.totalRows();
                    b     = f.releaseOwnership(this);
                    locked = lockAndGet();
                }
            }
        } finally {
            if (locked) unlock();
            if (eager) eager = false;
            if (b != null) Orphan.recycle(b);
            unpark(delayedWake);
        }
    }


    /* --- --- --- consumer methods --- --- --- */

    @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
        // always check READY before trying to acquire LOCK, since writers may hold it for > 1us
        @Nullable Thread me = null;
        B b = null, f;
        boolean locked = true;
        long parkNs;
        lock();
        try {
            while (true) {
                if ((b = ready) != null) {
                    ready = null;
                    break;
                } else if ((f=filling) != null) { // no ready, but has filling
                    if ((parkNs = readyInNanos(f.totalRows(), fillingStart)) == 0) {
                        b            = f;    // filling is ready,
                        filling      = null; // take it
                        fillingStart = Timestamp.ORIGIN;
                        break;
                    }
                } else if (plainState.isTerminated()) { // also: ready and filling are null
                    break;
                } else {   // start a filling batch using offer
                    parkNs = Long.MAX_VALUE;
                    if (offer != null) {
                        filling = offer.takeOwnership(this).clear(nColumns);
                        offer = null;
                    }
                    if (needsStartTime) fillingStart = nanoTime();
                }
                // park until time-based completion or unpark from offer()/copy()/cleanup()
                //dbg.write("nextBatch: parking, parkNs=", parkNs);
                consumer = me == null ? me = currentThread() : me;
                unlock();
                locked = false;
                if (parkNs == Long.MAX_VALUE) park(this);
                else                          parkNanos(this, parkNs);
                consumer = null;
                lock();
                locked = true;
            }
        } finally {
            if (locked) {
                if (b != null)
                    queuedRows -= b.totalRows();
                unlock();
            }
            // recycle offer if we did not already
            Orphan.safeRecycle(offer);
            unpark(producer);
        }

        if (b == null) // terminal batch
            return onTerminal(); // throw if failed
        offer = b.releaseOwnership(this);
        onNextBatch(offer); // guides getBatch() allocations
        return offer;
    }

    @SuppressWarnings("SameReturnValue") private @Nullable Orphan<B> onTerminal() {
        lock();
        B f = filling;
        filling = null;
        unlock();
        Batch.safeRecycle(f, this);
        checkError();
        return null;
    }
}
