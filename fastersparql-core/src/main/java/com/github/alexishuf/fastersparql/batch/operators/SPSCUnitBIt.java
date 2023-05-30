package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;

import static java.lang.invoke.MethodHandles.lookup;

/**
 * A single-producer, single-consumer {@link CallbackBIt} that holds at most one
 * queued {@link Batch}.
 *
 * <p>Unlike {@link SPSCBIt}, producer methods ({@link #offer(Batch)} and {@link #copy(Batch)})
 * will make batches ready ASAP and {@link #nextBatch(Batch)} will not try to steal filling
 * batches. This leads to slightly less contention between producer and consumer, but causes the
 * producer to be park/unparked more frequently.</p>
 */
public final class SPSCUnitBIt<B extends Batch<B>> extends AbstractBIt<B> implements CallbackBIt<B> {
    private static final VarHandle QS;

    static {
        try {
            QS = lookup().findVarHandle(SPSCUnitBIt.class, "queueState", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Set of bit flags that describe the queue-related state. See {@code QS_} constants. */
    @SuppressWarnings({"unused", "FieldMayBeFinal"}) // accessed through QS
    private int queueState = 0;

    private static final int QS_READY    = 1     ;
    private static final int QS_PKD_CONS = 1 << 1;
    private static final int QS_PKD_PROD = 1 << 2;
    private static final int QS_WRITING = 1 << 3;
    private static final int QS_TERM     = 1 << 4;
    private static final int QS_UBC = 1 << 5;
    private static final int QS_PKD      = QS_PKD_CONS|QS_PKD_PROD;

    private @Nullable B ready;
    private long fillingStart;
    private Thread consumer, producer;

//    private final DebugJournal.RoleJournal journal;

    public SPSCUnitBIt(BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
        //journal = DebugJournal.SHARED.role("SPSCUnitBIt@"+id());
    }

    /**
     * Tries to acquire the QS_LOCK once.
     *
     * @return current QS with {@code QS_LOCK} set iff owned by this thread
     */
    private int lockWriting(int qs) {
        qs &= ~QS_WRITING;
        for (int o; (o = (int)QS.compareAndExchange(this, qs, qs|QS_WRITING)) != qs;) {
            qs = o&~QS_WRITING;
            Thread.onSpinWait();
        }
        return qs|QS_WRITING;
    }

    /**
     * Sets ({@code set} and/or clears ({@code clear}) {@code QS_*} flags in QS. If a
     * {@code QS_PKD_*} flag gets cleared, the corresponding {@link Thread} will be
     * {@link LockSupport#unpark(Thread)}.
     *
     * @param qs    last observed state
     * @param set   bitset of flags to be set
     * @param clear bitset of flags to be cleared.
     * @return last observed state with requested modifications.
     */
    private int setQS(int qs, int set, int clear) {
        int nqs;
        for (int o; (o=(int)QS.compareAndExchange(this, qs, nqs=(qs|set)&~clear)) != qs ; qs = o)
            Thread.onSpinWait();
        // unpark only threads we saw parked and caller asked to unpark. this avoids self-unpark()
        if ((qs&clear&QS_PKD_CONS) != 0) LockSupport.unpark(consumer);
        if ((qs&clear&QS_PKD_PROD) != 0) LockSupport.unpark(producer);
        return nqs;
    }

    /* --- --- --- overridden methods --- --- --- */

    @Override protected void cleanup(@Nullable Throwable cause) {
        setQS((int)QS.getOpaque(this), QS_TERM, QS_PKD);
        super.cleanup(cause);
        //journal.close();
    }

    @Override protected void updatedBatchConstraints() {
        super.updatedBatchConstraints();
        // toggling QS_UBC ensures a write that will make writes from setters visible to
        // producer and consumer
        int qs = (int) QS.getOpaque(this);
        setQS(qs, (qs^QS_UBC)&QS_UBC, QS_PKD);
    }

    @Override public boolean isCompleted() {
        return ((int)QS.getAcquire(this)&QS_TERM) != 0;
    }

    @Override public boolean isFailed() {
        return ((int)QS.getAcquire(this)&QS_TERM) != 0 && error != null;
    }

    /* --- --- --- consumer methods --- --- --- */

    @Override public B nextBatch(@Nullable B offer) {
        offer = recycle(offer);
        int qs = (int) QS.getOpaque(this);
        if (((qs&(QS_TERM|QS_READY)) == 0))
            consumer = Thread.currentThread();
        // wait until ready or terminated
        while ((qs&(QS_TERM|QS_READY)) == 0) {
            long parkNs = Long.MAX_VALUE;
            if (needsStartTime) {
                int set = QS_PKD_CONS;
                qs = lockWriting(qs); // forbids offer()/copy() from appending rows
                try {
                    if ((qs & (QS_TERM | QS_READY)) != 0) {
                        set = 0; // will not park
                        break;   // cleanup()/offer()/copy() ran before we locked
                    } else {
                        B b = ready;
                        if (b == null) {
                            fillingStart = Timestamp.nanoTime(); // start counting time
                        } else if ((parkNs = readyInNanos(b.rows, fillingStart)) == 0) {
                            set = QS_READY; // stole ready, will not park
                            fillingStart = Timestamp.ORIGIN;
                            break;
                        }
                    }
                } finally {
                    qs = setQS(qs, set, QS_PKD_PROD|QS_WRITING);
                }
            } else { // !needsStartTime
                int ex = qs;
                if ((qs = (int) QS.compareAndExchange(this, ex, ex|QS_PKD_CONS)) != ex)
                    continue; // producer changed state, check instead of parking
            }
            //journal.write("nextBatch park ns=", parkNs);
            // park (if QS_TERM, QS_READ or stolen QS_READ, we already quit this loop)
            if (parkNs == Long.MAX_VALUE) LockSupport.park(this);
            else                          LockSupport.parkNanos(this, parkNs);
            // re-read state after unpark()
            qs = (int)QS.getOpaque(this);
        }

        // retry recycle(offer) and delegate to batchType if rejected
        if (recycle(offer) != null) batchType.recycle(offer);


        if ((qs&QS_READY) == 0) { //no batch, implies QS_TERM
            //journal.write("nextBatch exhausted");
            checkError(); // throw if complete(error)
            return null;  // reached if complete(null)
        }

        //take batch
        B b = ready;
        //journal.write("SPSCUnitBIt.nextBatch &b=", identityHashCode(b), "rows=", b == null ? 0 : b.rows, "[0][0]=", b == null ? null : b.get(0, 0));
        ready = null;
        eager = false;
        onBatch(b);
        setQS(qs, 0, QS_READY|QS_PKD_PROD); // unpark()s producer if parked.
        return b;
    }

    /* --- --- --- producer methods --- --- --- */

    @Override public B offer(B b) throws BItCompletedException {
        int qs = (int) QS.getOpaque(this);
        if ((qs&(QS_READY|QS_TERM)) == QS_READY)
            producer = Thread.currentThread();
        //wait until !QS_READY or QS_TERM
        for (int o; (qs&(QS_READY|QS_TERM)) == QS_READY; qs = o) {
            if ((o = (int)QS.compareAndExchange(this, qs, qs|QS_PKD_PROD)) == qs) {
                //journal.write("offer: park rows=", b.rows, "&offer=", bId, "[0][0]=", b.get(0, 0));
                LockSupport.park(this);
                o = (int)QS.getOpaque(this);
            }
        }

        qs = lockWriting(qs); // spin while nextBatch() is stealing
        //journal.write("offer: lck rows=", b.rows, "&offer=", bId, "[0][0]=", b.get(0, 0));
        int setFlags = 0;
        B recycled = null;
        try {
            if ((qs & QS_TERM) != 0) throw mkCompleted(); // reject offer() after complete()
            B filling = ready;
            if (filling == null) {
                if (needsStartTime) fillingStart = Timestamp.nanoTime();
                // b.rows < minBatch likely means we have a fill-by-row producer.
                // copying to a recycled batch will avoid a realloc on next offer()/copy()
                // and will avoid a new alloc for the producer.
                if (b.rows < minBatch && (filling = stealRecycled()) != null) {
                    filling.clear(vars.size());
                    //journal.write("SPSCUnitBIt.offer: put() on steal &b=", identityHashCode(b), "b.rows=", b.rows, "[0][0]=", b.get(0, 0));
                    filling.put(recycled = b);
                } else {
                    //journal.write("SPSCUnitBIt.offer: set ref &b=", identityHashCode(b), "b.rows=", b.rows, "[0][0]=", b.get(0, 0));
                    filling = b;
                }
                ready = filling;
            } else {
                //journal.write("SPSCUnitBIt.offer: put() &b=", identityHashCode(b), "b.rows=", b.rows, "[0][0]=", b.get(0, 0));
                filling.put(recycled = b);
            }
            if (readyInNanos(filling.rows, fillingStart) == 0) {
                //journal.write("SPSCUnitBIt.offer: pub &f=", identityHashCode(filling), "f.rows=", filling.rows, filling==ready?"f==ready":"f!=ready");
                setFlags = QS_READY;
                fillingStart = Timestamp.ORIGIN;
            }
        } finally { // set ready, unlock and unpark consumer
            //journal.write("SPSCUnitBIt.offer: rls rows=", b.rows, "&offer=", bId, "[0][0]=", b.get(0, 0));
            setQS(qs, setFlags, QS_WRITING|QS_PKD_CONS);
        }
        //journal.write("SPSCUnitBIt.offer: ret &recycled=", identityHashCode(recycled), "&b=", identityHashCode(b));
        return recycled == null ? stealRecycled() : recycled;
    }

    public void copy(B src) {
        int qs = (int) QS.getOpaque(this);
        if ((qs&(QS_READY|QS_TERM)) == QS_READY)
            producer = Thread.currentThread();
        //wait until !QS_READY or QS_TERM
        for (int o; (qs&(QS_READY|QS_TERM)) == QS_READY; qs = o) {
            if ((o = (int)QS.compareAndExchange(this, qs, qs|QS_PKD_PROD)) == qs) {
                //journal.write("copy: park");
                LockSupport.park(this);
                o = (int)QS.getOpaque(this);
            }
        }

        qs = lockWriting(qs); // spin while nextBatch() is stealing
        int setFlags = 0;
        try {
            if ((qs & QS_TERM) != 0) throw mkCompleted(); // reject copy() after complete()
            B filling = ready;
            if (filling == null) {
                //journal.write("copy: start filling from src.rows=", src.rows);
                if (needsStartTime) fillingStart = Timestamp.nanoTime();
                ready = filling = getBatch(null);
            } //else { journal.write("copy: put() rows=", src.rows, "on filling.rows=", filling.rows); }
            filling.put(src);
            if (readyInNanos(filling.rows, fillingStart) == 0) {
                //journal.write("copy: publish filling.rows=", filling.rows, "[0][0]=", requireNonNull(filling.get(0, 0)).local[1]-'0');
                setFlags = QS_READY;
                fillingStart = Timestamp.ORIGIN;
            }
        } finally { // set ready, unlock and unpark consumer
            setQS(qs, setFlags, QS_WRITING|QS_PKD_CONS);
        }
    }

    /* --- --- --- CallbackBIt methods --- --- --- */

    @Override public int                  maxReadyBatches()           { return 1; }
    @Override public int maxReadyItems()           { return Integer.MAX_VALUE; }
    @Override public @This CallbackBIt<B>   maxReadyItems(int items) { return this; }
    @Override public void         complete(@Nullable Throwable error) { onTermination(error); }
}
