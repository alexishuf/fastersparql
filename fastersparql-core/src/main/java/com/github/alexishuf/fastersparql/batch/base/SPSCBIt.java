package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.CallbackBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Integer.lowestOneBit;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Long.MAX_VALUE;
import static java.lang.System.nanoTime;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * A {@link CallbackBIt} for a single producer thread and a single consumer thread, built
 * around a mostly non-blocking queue of fixed size.
 */
public class SPSCBIt<B extends Batch<B>> extends AbstractBIt<B> implements CallbackBIt<B> {
    private static final VarHandle QS, REC_BS;
    private static final VarHandle QUEUE = MethodHandles.arrayElementVarHandle(Batch[].class);

    static {
        try {
            QS = lookup().findVarHandle(SPSCBIt.class, "queueState", long.class);
            REC_BS = lookup().findVarHandle(SPSCBIt.class, "recycledBitset", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * All state shared producer and consumer threads, packed in a 64-bit value.
     *
     * <p><strong>Only access through {@link SPSCBIt#QS}</strong>. See {@code QS_*}
     * constants on how to destruct/construct/modify states. Layout:</p>.
     *
     * <pre>
     *  +-----------+-----------+----------+-------------+
     *  | items(32) |  flags(8) | wIdx(12) | batches(12) |
     *  | 62      32|31       24|23      12|11          0| bit indices
     *  +-----------+-----------+----------+-------------+
     * </pre>
     */
    @SuppressWarnings({"FieldMayBeFinal", "unused"}) // accessed through QS
    private long queueState;
    /** nextBatch() will read its next batch from queue[rIdx]. Only accessed from nextBatch() */
    private int rIdx = 0;
    /**
     * If bit {@code i} is set, queue[queueMask+1+i] holds a recycled batch that offer()
     * may consume
     */
    @SuppressWarnings("FieldMayBeFinal") // accessed through OIDX
    private int recycledBitset = 0;
    private @Nullable Thread consumer, producer;


    private static final int QS_BATCHES_BIT = 0;
    private static final int QS_WIDX_BIT    = 12;
    private static final int QS_FLAGS_BIT   = 24;
    private static final int QS_ITEMS_BIT   = 32;

    private static final long QS_BATCHES_MASK = 0x0000000000000fffL;
    private static final long QS_WIDX_MASK    = 0x0000000000fff000L;
    private static final long QS_ITEMS_MASK   = 0xffffffff00000000L;

    /** cleanup() ran: do not accept offer()s nor park() in nextBatch()*/
    private static final int QS_TERM     = 1 << (QS_FLAGS_BIT  );
    /** nextBatch() park()ed or will park()*/
    private static final int QS_PKD_CONS = 1 << (QS_FLAGS_BIT+1);
    /** offer() park()ed or will park() */
    private static final int QS_PKD_PROD = 1 << (QS_FLAGS_BIT+2);
    /** offer() is writing on wIdx: nextBatch() cannot steal  */
    private static final int QS_WRITING  = 1 << (QS_FLAGS_BIT+3);
    /** updatedBatchConstraints() toggles this to make other writes visible */
    private static final int QS_UBC = 1 << (QS_FLAGS_BIT+5);
    private static final int QS_PKD = QS_PKD_CONS | QS_PKD_PROD;

    private final B[] queue;
    private final int queueMask;
    private long fillingStart = ORIGIN;
    private int maxReadyItems = Integer.MAX_VALUE;
//    private final DebugJournal.RoleJournal journal;

    /* --- --- --- constructors --- --- --- */

    public SPSCBIt(BatchType<B> batchType, Vars vars, int maxBatches) {
        super(batchType, vars);
        maxBatches = Math.min((int)QS_BATCHES_MASK+1, maxBatches);
        maxBatches = 1 + Math.max(1, -1 >>> Integer.numberOfLeadingZeros(maxBatches-1));
        int nRecycled = Math.min(32, maxBatches);
        //noinspection unchecked
        queue = (B[]) Array.newInstance(batchType.batchClass(), maxBatches+nRecycled);
        queueMask = maxBatches-1;
//        journal = DebugJournal.SHARED.role(getClass().getSimpleName()+'@'+id());
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isCompleted() {
        return ((long)QS.getAcquire(this)&QS_TERM) != 0;
    }

    public boolean isFailed() {
        return ((long)QS.getAcquire(this)&QS_TERM) != 0 && error != null;
    }

    /* --- --- --- helper methods --- --- --- */

    /** Tries observing {@code QS_WRITING} unset and setting it.
     *
     * @param ex current state
     * @return current state with {@code QS_WRITING} set iff it was set by this call.
     */
    private long lockWriting(long ex) {
        ex &= ~QS_WRITING;
        for (long o; (o = (long)QS.compareAndExchange(this, ex, ex|QS_WRITING)) != ex; ) {
            ex = o&~QS_WRITING;
            Thread.onSpinWait();
        }
        return ex|QS_WRITING;
    }

    /**
     * Compute {@code add} for an {@code setQS(qs, QS_WIDX_MASK, add)} call intended to increment
     * {@code items}, {@code wIdx} and {@code batches} components of {@link #queueState}.
     *
     * @param rows number of rows in batch made ready ({@code queue[newWIdx-1&queueMask].rows}
     * @param newWIdx already incremented wIdx
     * @return {@code add} parameter for {@code setQS(qs, QS_WIDX_MASK, add)}.
     */
    private long wIdxDelta(int rows, int newWIdx) {
        fillingStart = ORIGIN;
        return (long)rows << QS_ITEMS_BIT | (long)newWIdx << QS_WIDX_BIT | 1L << QS_BATCHES_BIT;
    }

    /** Spins until atomically sets {qs} to {@code (qs & ~clear) + add} */
    private long setQS(long qs, long clear, long add) {
        long o, n;
        for (; (o = (long)QS.compareAndExchange(this, qs, n=(qs&~clear)+add)) != qs; qs = o)
            Thread.onSpinWait();
        if ((o&clear&QS_PKD_CONS) != 0) LockSupport.unpark(consumer);
        if ((o&clear&QS_PKD_PROD) != 0) LockSupport.unpark(producer);
        return n;
    }

    @SuppressWarnings("unused")
    private static String qsString(long qs) {
        StringBuilder sb = new StringBuilder()
                .append("{items=").append(qs >>> QS_ITEMS_BIT)
                .append(", wIdx=").append((qs&QS_WIDX_MASK) >>> QS_WIDX_BIT)
                .append(", batches=").append(qs&QS_BATCHES_MASK >>> QS_BATCHES_BIT)
                .append(", ");
        if ((qs&QS_TERM)     != 0) sb.append("QS_TERM|");
        if ((qs&QS_PKD_CONS) != 0) sb.append("QS_PKD_CONS|");
        if ((qs&QS_PKD_PROD) != 0) sb.append("QS_PKD_PROD|");
        if ((qs&QS_WRITING)  != 0) sb.append("QS_WRITING|");
        if ((qs& QS_UBC)      != 0) sb.append("QS_UBC|");
        sb.setLength(sb.length()-(sb.charAt(sb.length()-1) == '|' ? 1 : 2));
        return sb.append('}').toString();
    }

    /* --- --- --- configurable behavior --- --- --- */

    /**
     * Subclasses may implement this, returning false to allow an offer() above capacity
     * to continue without blocking.
     */
    protected boolean blocksOnNoCapacity() {
        return true;
    }

    /** Whether this iterator has any capacity left for an {@link #offer(Batch)} call. */
    protected final boolean hasCapacity() {
        return hasCapacity((long)QS.getAcquire(this), 1);
    }
    private boolean hasCapacity(long qs, int offer) {
        B filling = queue[((int) qs & (int) QS_WIDX_MASK) >> QS_WIDX_BIT];
        int fRows = filling == null ? 0 : filling.rows;
        int items = fRows + (int)((qs & QS_ITEMS_MASK) >>> QS_ITEMS_BIT);
        if (items > 0 && items+offer > maxReadyItems)             return false;
        if (((qs&QS_BATCHES_MASK)>>>QS_BATCHES_BIT) == queueMask) return fRows+offer <= maxBatch;
        return true; // batches < queueMask && items+offer <= maxReadyItems
    }

    /* --- --- --- overrides --- --- --- */

    @SuppressWarnings("unchecked") @Override public @Nullable B stealRecycled() {
        B b;
        if ((b = (B)RECYCLED.getAndSetAcquire(this, null)) != null) {
            //journal.write("stealRecycled R: &b=", identityHashCode(b), "rows=", b.rows, "[0][0]=", b.rows == 0 ? null : b.get(0, 0));
            return b;
        }
        for (int i = 0, bs; (i+=numberOfTrailingZeros((bs=(int)REC_BS.getAcquire(this))>>>i)) < 32 ; i++) {
            int qi = i +queueMask+1;
            if (qi >= queue.length) break;
            if ((b = (B)QUEUE.getAndSetAcquire(queue, qi, null)) != null) {
                // clear bit i at REC_BS, signalling we took the batch at queue[qi]
                int bit = 1<<i;
                for (int e; (bs=(int)REC_BS.compareAndExchange(this, e=bs, bs&~bit)) != e; )
                    Thread.onSpinWait();
                //journal.write("stealRecycled queue: &b=", identityHashCode(b), "rows=", b.rows, "[0][0]=", b.rows == 0 ? null : b.get(0, 0));
                return b;
            }
        }
        return null;

//        int i = numberOfTrailingZeros(bs), bit = 1<<i;
//        i += queueMask+1;
//        if (i < queue.length) {
//            b = queue[i];
//            queue[i] = null;
//            while ((i = (int)REC_BS.compareAndExchangeRelease(this, bs|bit, bs&~bit)) != bs)
//                bs = i;
//        } else { //noinspection unchecked
//            b = (B)RECYCLED.getAndSetAcquire(this, null);
//        }
//        if (b != null) {
//            b.changeThread(this);
//            journal.write("stealRecycled: ret &b=", identityHashCode(b));
//        }
//        return b;
    }

    @SuppressWarnings("unchecked") @Override public @Nullable B recycle(B offer) {
        if (offer == null) return null;
        if (RECYCLED.compareAndExchangeRelease(this, null, offer) == null) {
            //journal.write("recycle to R, &b=", identityHashCode(offer), "rows=", offer.rows, "[0][0]=", offer.rows == 0 ? null : offer.get(0, 0));
            return null;
        }
        queue_recycle:
        while (true) {
            int bs = (int) REC_BS.getAcquire(this);
            int i; // atomically find  bit i unset and set it.
            while (true) {
                int bit = lowestOneBit(~bs), e;
                if (bit == 0) break queue_recycle; // no space
                if ((bs = (int)REC_BS.compareAndExchange(this, e=bs&~bit, bs|bit)) == e) {
                    i = numberOfTrailingZeros(bit); break;
                }
                Thread.onSpinWait();
            }
            if ((i += queueMask+1) >= queue.length)
                break; // no space
            if ((B)QUEUE.compareAndExchangeRelease(queue, i, null, offer) == null) {
                //journal.write("recycled to queue, &b=", identityHashCode(offer), "rows=", offer.rows, "[0][0]=", offer.rows == 0 ? null : offer.get(0, 0));
                return null; // stored
            }
            Thread.onSpinWait();
        }

        return offer;

//        BIt<B> oldIt = offer.ownerIt;
//        offer.changeIt(this);
//        offer.clear();
//        int bs = (int) REC_BS.getAcquire(this);
//        int i = numberOfTrailingZeros(~bs), bit = 1<<i;
//        i += queueMask+1;
//        if (i < queue.length) {
//            queue[i] = offer; // store after ready queue
//            while ((i = (int) REC_BS.compareAndExchangeRelease(this, bs, bs|bit)) != bs)
//                bs = i;
//            return null;
//        }
//        if (RECYCLED.compareAndExchangeRelease(this, null, offer) == null)
//            return null;
//        offer.changeIt(oldIt);
//        return offer;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        // If there is a filling batch, must make it ready for nextBatch()
        // In any case, must set QS_TERM and awake both producer and consumer
        long qs = lockWriting((long) QS.getOpaque(this));
        long wIdxDelta = qs&QS_WIDX_MASK;
        try {
            int wi = (int)wIdxDelta>>QS_WIDX_BIT;
            B b = queue[wi];
            if (b != null && b.rows > 0) {
                wIdxDelta = wIdxDelta(b.rows, wi + 1 & queueMask);
            }
        } finally {
            setQS(qs, QS_PKD|QS_WIDX_MASK|QS_WRITING|QS_TERM, wIdxDelta|QS_TERM);
        }
        //journal.write("cleanup: begin clearing recycled");
        //send all recycled batches we hold to batchType's pool
        for (B b; (b = stealRecycled()) != null; )
            batchType.recycle(b);
        //journal.write("cleanup: end clearing recycled");
        super.cleanup(cause);
//        journal.close();
    }

    @Override protected void updatedBatchConstraints() {
        super.updatedBatchConstraints();
        long qs = (long)QS.getOpaque(this);
        setQS(qs, QS_PKD, (qs& QS_UBC) == 0 ? QS_UBC : -QS_UBC);
    }

    /* --- --- --- implementations --- --- --- */

    @Override public int maxReadyBatches() { return queue.length; }

    @Override public @This SPSCBIt<B> maxReadyItems(int max) {
        this.maxReadyItems = max;
        updatedBatchConstraints();
        return this;
    }

    @Override public int maxReadyItems() { return maxReadyItems; }

    @Override public void complete(@Nullable Throwable error) {
        onTermination(error);
    }

    @Override public B offer(B offer) throws BItCompletedException {
        int offerSize = offer.rows;
        //journal.write("SPSCBIt.offer: &offer=", identityHashCode(offer), "rows=", offerSize, "[0][0]=", offerSize == 0 ? null : offer.get(0, 0));
        if (offerSize == 0)
            return offer;
        Thread me = null;
        long qs = (long)QS.getOpaque(this);

        // wait until complete() or free capacity
        for (long qs_; (qs&QS_TERM) == 0; qs = qs_) {
            if (!hasCapacity(qs, offerSize) && blocksOnNoCapacity()) {
                if (me == null) producer = me = Thread.currentThread();
                if ((qs_ = (long)QS.compareAndExchange(this, qs, qs|QS_PKD_PROD)) == qs) {
                    //journal.write("SPSCBIt.offer: park() rows=", offerSize, "&offer=", offerId, "[0][0]=", offer.get(0, 0));
                    LockSupport.park(this);
                    qs_ = (long) QS.getOpaque(this); //re-read state after unpark()
                }
            } else {
                break; // hasCapacity() or subclass handled it at blocksOnNoCapacity()
            }
        }

        B recycled = null;
        // spin until nextBatch() or cleanup() finish stealing wIdx
        long wIdxDelta = (qs = lockWriting(qs))&QS_WIDX_MASK;
        //journal.write("SPSCBIt.offer: lck rows=", offerSize, "&offer=", offerId, "[0][0]=", offer.get(0, 0));
        try {
            if ((qs&QS_TERM) != 0) throw mkCompleted();

            //write batch to queue[wIdx] (or queue[wIdx+1])
            int incCapacity = queueMask-((int)qs&(int)QS_BATCHES_MASK)>>QS_BATCHES_BIT;
            int wIdx = (int)wIdxDelta>>QS_WIDX_BIT;
            B f = queue[wIdx];
            int fRows = f == null ? 0 : f.rows;
            if (fRows == 0) { //not filling (first call or stolen by nextBatch())
                if (needsStartTime && fillingStart == ORIGIN) fillingStart = nanoTime();
                queue[wIdx] = offer;
                //journal.write("SPSCBIt.offer: store ref at wIdx=", wIdx, "[0][0]=", offer.get(0, 0));
            } else if (incCapacity == 0 || readyInNanos(fRows, fillingStart) > 0) { // filling not ready
                // f is not ready, or we can't increment wIdx and thus must put()
                f.put(offer);
                //journal.write("SPSCBIt.offer: put() at wIdx=", wIdx, "[0][0]=", offer.get(0, 0));
                recycled = offer;
            } else if (incCapacity <= queueMask>>1 && f.offer(offer)) { //ready, faster than consumer
                // f is ready, but are running low on incCapacity and f accepted the offer()
                //journal.write("SPSCBIt.offer: offer() at wIdx=", wIdx, "[0][0]=", offer.get(0, 0));
                recycled = offer;
            } else { // filling is ready, and we are not running low on incCapacity
                fillingStart = ORIGIN;
                queue[wIdx = wIdx + 1 & queueMask] = offer;
                //journal.write("SPSCBIt.offer: inc wIdx=", wIdx, "[0][0]=", offer.get(0, 0));
                wIdxDelta = wIdxDelta(fRows, wIdx);
            }
        } finally {
            //journal.write("SPSCBIt.offer: rls rows=", offerSize, "&offer=", offerId, "[0][0]=", offer.get(0, 0));
            // unlock QS_WRITING, unpark consumer and maybe ++wIdx, ++batches, items += fRows.
            setQS(qs, QS_WRITING|QS_WIDX_MASK|QS_PKD_CONS, wIdxDelta);
        }

        //journal.write("SPSCBIt.offer: &offer=", identityHashCode(offer),  " return &recycled=", identityHashCode(recycled));
        return recycled == null ? stealRecycled() : recycled;
    }

    @Override public B nextBatch(@Nullable B offer) {
        if (offer != null)
            offer = recycle(offer);
        long qs = (long) QS.getOpaque(this);
        if ((qs & (QS_BATCHES_MASK | QS_TERM)) == 0)
            consumer = Thread.currentThread(); //only needs to write once

        // wait until queue not empty or a this.complete() call.
        for (long parkNs = MAX_VALUE; (qs & (QS_BATCHES_MASK | QS_TERM)) == 0;) {
            qs = lockWriting(qs); // no batch ready try stealing
            long addBits = qs & QS_WIDX_MASK; // leave items, batches and wIdx unchanged
            try {
                if ((qs&(QS_BATCHES_MASK|QS_TERM)) != 0)
                    break; // offer()/complete() during/before lockWriting()
                int wi = (int) addBits >>> QS_WIDX_BIT;
                addBits |= QS_PKD_CONS; // looks like we will park()...
                B b = queue[wi];
                if (b == null) { // no filling batch (implies fillingStart == ORIGIN_TIMESTAMP)
                    fillingStart = System.nanoTime(); // start counting time
                } else if ((parkNs = readyInNanos(b.rows, fillingStart)) == 0) {
                    addBits = wIdxDelta(b.rows, wi + 1 & queueMask); // stole
                    fillingStart = ORIGIN;
                    break; // stole filling batch
                }
            } finally {
                qs = setQS(qs, QS_PKD|QS_WIDX_MASK|QS_WRITING, addBits);
            }
            //journal.write("nextBatch park ns=", parkNs);
            // if wIdx was not stolen, park() until time-based ready or producer does something
            if   (parkNs == MAX_VALUE) LockSupport.park(this);
            else                       LockSupport.parkNanos(this, parkNs);
            qs = (long) QS.getAcquire(this); // must re-read state after park, oqs is old
        }

        if (offer != null) { //recycle if rejected, offer() must have cleared a slot
            if ((qs & QS_TERM) ==    0) offer = recycle(offer);
            if ( offer         != null) batchType.recycle(offer);
        }

        final int rIdx = this.rIdx;
        B b = queue[rIdx];
        if (b == null) {
            //journal.write("SPSCBIt.nextBatch: exhausted rIdx=", rIdx, "qs=", qsString(qs));
            checkError(); // throws if complete(error)
            return null;
        }
        //journal.write("nextBatch: ", b.toString());
        //journal.write("SPSCBIt.nextBatch: rIdx=", rIdx, "rows=", b.rows, "[0][0]=", b.get(0, 0));
        //journal.write("SPSCBIt.nextBatch: &b=", identityHashCode(b));
        queue[rIdx] = null;
        this.rIdx = rIdx + 1 & queueMask;
        this.eager = false;
        // consume batch (--batch), items (items -= b.rows), and unpark() producer
        setQS(qs, QS_PKD_PROD, ((long)-b.rows<<QS_ITEMS_BIT) + (-1<<QS_BATCHES_BIT));
        adjustCapacity(b);

        return b;
    }

}
