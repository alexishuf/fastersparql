package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.BItCancelledException;
import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.exceptions.FSCancelledException;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.Timestamp.ORIGIN;
import static com.github.alexishuf.fastersparql.batch.Timestamp.nanoTime;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
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
        FSException e = ack ? new FSCancelledException()
                            : new FSServerException("server spontaneously cancelled");
        return complete(e);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        try {
            if (cause instanceof BItCancelledException) { // drop all queued
                ready = batchType.recycle(ready);
                filling = batchType.recycle(filling);
            } else if (filling != null) {
                if (filling.rows == 0)  // offer from nextBatch, recycle
                    batchType.recycle(filling);
                else // promote to ready
                    ready   = Batch.quickAppend(ready, filling);
                filling = null;
            }
            super.cleanup(cause);
        } finally {
            unpark(producer);
            unpark(consumer);
        }
    }

    /* --- --- --- producer methods --- --- --- */

    private B fillingTail() {
        B f = filling, tail = null;
        if (f != null) {
            if ((tail = f.detachTail()) == f)
                this.filling = null;
            queuedRows -= tail.rows;
        }
        return tail;
    }

    @Override public B fillingBatch() {
        lock();
        try {
            B tail = fillingTail();
            return tail == null ? batchType.create(nColumns) : tail;
        } finally { unlock(); }
    }

    protected final boolean   lockAndGet() {   lock(); return  true; }
    protected final boolean unlockAndGet() { unlock(); return false; }

    @Override public @Nullable B offer(B b) throws TerminatedException, CancelledException {
        Thread delayedWake = null;
        int bRows = b.totalRows();
        boolean locked = lockAndGet();
        try {
            while (b != null) {
                if (plainState.isTerminated()) {
                    batchType.recycle(b);
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
                        ready = b;
                        fillingStart = ORIGIN;
                    } else {
                        filling = Batch.quickAppend(filling, b);
                    }
                    queuedRows += bRows;
                    delayedWake = consumer;
                    b = null;
                } else  { // append b to filling while unlocked
                    B f = fillingTail();
                    locked = unlockAndGet();
                    f.append(b);
                    bRows  = (b=f).totalRows();
                    locked = lockAndGet();
                }
            }
        } finally {
            if (locked) unlock();
            if (eager) eager = false;
            unpark(delayedWake);
        }
        return b; // b is always null, by design
    }

    @Override public void copy(B b) throws TerminatedException, CancelledException {
        B filling = fillingBatch();
        filling.copy(b);
        filling = offer(filling);
        if (filling != null)
            batchType.recycle(filling);
    }

    /* --- --- --- consumer methods --- --- --- */

    @Override public @Nullable B nextBatch(@Nullable B offer) {
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
                        offer.requireUnpooled();
                        filling = offer.clear(nColumns);
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
            batchType.recycle(offer);
            unpark(producer);
        }

        if (b == null) // terminal batch
            return onTerminal(); // throw if failed
        onNextBatch(b); // guides getBatch() allocations
        return b;
    }

    @SuppressWarnings("SameReturnValue") private @Nullable B onTerminal() {
        lock();
        B f = filling;
        filling = null;
        unlock();
        Batch.recycle(f);
        checkError();
        return null;
    }
}
