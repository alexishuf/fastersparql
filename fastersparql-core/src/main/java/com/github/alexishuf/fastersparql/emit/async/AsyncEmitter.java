package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.recyclePooled;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.THREAD_JOURNAL;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Thread.onSpinWait;

/**
 * An emitter that feeds 1+ {@link Receiver}s from 1+ {@link Producer}s,
 * decoupling time coupling of both sets: producers can produce concurrently with
 * receivers consumption.
 *
 * @param <B> the batch type
 */
@SuppressWarnings("unchecked")
public final class AsyncEmitter<B extends Batch<B>> extends TaskEmitter<B> implements BatchQueue<B>  {
    private static final Logger log = LoggerFactory.getLogger(AsyncEmitter.class);

    private static final Object[] NO_PEERS = new Object[0];
    private static final int PROD_NUDGE = 0;
    private static final int RECV_NUDGE = 1;
    private static final int PACKED_PEERS = 12;

    private static final VarHandle READY, RECYCLED0, RECYCLED1, TERM_PRODS;
    private static final VarHandle[] PROD = new VarHandle[PACKED_PEERS];
    private static final VarHandle[] RECV = new VarHandle[PACKED_PEERS];
    static {
        var suffixes = IntStream.range(0, 12).mapToObj(i -> Integer.toHexString(i).toUpperCase())
                                .toArray(String[]::new);
        try {
            READY      = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainReady", Batch.class);
            RECYCLED0  = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRecycled0", Batch.class);
            RECYCLED1  = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRecycled1", Batch.class);
            TERM_PRODS = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainTermProds", int.class);
            for (int i = 0; i < suffixes.length; i++) {
                PROD[i] = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "producer"+suffixes[i], Producer.class);
                RECV[i] = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "receiver"+suffixes[i], Receiver.class);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    short nProducers, nReceivers;
    @SuppressWarnings("unused") private @Nullable B plainReady;
    private @Nullable B filling;
    @SuppressWarnings("unused") private @Nullable B plainRecycled0, plainRecycled1;
    private @Nullable B scatterRecycled;
    @SuppressWarnings("unused") private Receiver<B> receiver0, receiver1;
    @SuppressWarnings("unused") private Producer<B> producer0, producer1;
    @SuppressWarnings("unused") private Receiver<B> receiver2, receiver3;
    @SuppressWarnings("unused") private Producer<B> producer2, producer3;
    @SuppressWarnings("unused") private Receiver<B> receiver4, receiver5;
    @SuppressWarnings("unused") private Producer<B> producer4, producer5;
    @SuppressWarnings("unused") private Receiver<B> receiver6, receiver7;
    @SuppressWarnings("unused") private Producer<B> producer6, producer7;
    @SuppressWarnings("unused") private Receiver<B> receiver8, receiver9;
    @SuppressWarnings("unused") private Producer<B> producer8, producer9;
    @SuppressWarnings("unused") private Receiver<B> receiverA, receiverB;
    @SuppressWarnings("unused") private Producer<B> producerA, producerB;
    @SuppressWarnings("unused") private int plainTermProds;
    private Object[] extraPeers = NO_PEERS;

    public AsyncEmitter(BatchType<B> batchType, Vars vars) {
        this(batchType, vars, EmitterService.EMITTER_SVC);
    }

    public AsyncEmitter(BatchType<B> batchType, Vars vars, EmitterService runner) {
        super(batchType, vars, runner, RR_WORKER, CREATED, TASK_FLAGS);
    }

    @Override protected void doRelease() {
        RECYCLED0.setOpaque(this, recyclePooled((B)RECYCLED0.getOpaque(this)));
        RECYCLED1.setOpaque(this, recyclePooled((B)RECYCLED1.getOpaque(this)));
        scatterRecycled = recyclePooled(scatterRecycled);
        super.doRelease();
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.of(
                producer0, producer1, producer2, producer3, producer4, producer5,
                producer6, producer7, producer8, producer9, producerA, producerB
        ).filter(Objects::nonNull);
    }

    /* --- --- --- internals shared by producer and consumer --- --- --- */

    private int lockForRegister() {
        int s = lock(statePlain());
        if ((s&IS_INIT) == 0) {
            unlock(s);
            throw new RegisterAfterStartException(this);
        }
        return s;
    }

    private Producer<B> producer(int i) {
        if (i < PROD.length)
            return (Producer<B>) PROD[i].get(this);
        return (Producer<B>) extraPeers[((i-PACKED_PEERS)<<1) + PROD_NUDGE];
    }

    private void addExtraPeer(Object peer, int n, int nudge) {
        int idx = ((n-PACKED_PEERS)<<1) + nudge;
        if (idx >= extraPeers.length)
            extraPeers = Arrays.copyOf(extraPeers, Math.max(32, extraPeers.length<<1));
        extraPeers[idx] = peer;
    }

    /* --- --- --- producer methods --- --- --- */

    /**
     * Register a {@link Producer} that will feed this {@link AsyncEmitter} via
     * {@link #offer(Batch)}.
     *
     * @param producer the Producer instance.
     * @throws RegisterAfterStartException if this called after the first {@link #request(long)}.
     */
    public void registerProducer(Producer<B> producer) throws RegisterAfterStartException {
        if (THREAD_JOURNAL)
            journal("register", producer, "on", this);
        int state = lockForRegister();
        try {
            short n = nProducers;
            if (n < PACKED_PEERS) PROD[n].set(this, producer);
            else                  addExtraPeer(producer, n, PROD_NUDGE);
            nProducers = (short) (n+1);
        } finally {
            unlock(state);
        }
    }

    /**
     * Wrap and {@link #registerProducer(Producer)} {@code upstream} or steals the producers from
     * {@code upstream}.
     *
     * <p>Upstream is compatible and has no {@link Receiver}s, it may be unwrapped and have its
     * internals stolen to become producers of this {@link AsyncEmitter}. If that happens,
     * {@code upstream} is left in a failed state, as if it had experienced an error already
     * delivered to the {@link Receiver#onError(Throwable)} of a {@link Receiver}. Therefore
     * callers of this method must ensure that either:</p>
     *
     * <ol>
     *     <li>{@code upstream} already has some {@link Receiver}
     *         {@link Emitter#subscribe(Receiver)} (to curtail the stealing)</li>
     *     <li>Ensure that no references to {@code upstream} remain other than the one given to
     *         this call.</li>
     * </ol>
     *
     * @param up The upstream emitter to be subscribed to. If required, batches will
     *                 be converted and projected before entering this {@link AsyncEmitter}.
     */
    public <I extends Batch<I>> void registerProducer(Emitter<I> up) {
        if (THREAD_JOURNAL) journal("regProd", up, "on", this);
        BatchMerger<B> proj = batchType.projector(vars, up.vars());
        if (up instanceof BItEmitter<I> be && proj == null && batchType.equals(up.batchType())) {
            new BItProducer<>(((BItEmitter<B>)be).stealIt(), this);
        } else {
            Emitter<B> sameType;
            if (!batchType.equals(up.batchType())) {
                sameType = batchType.convert(up);
            } else {
                if (proj == null && up instanceof AsyncEmitter<I> ae && ae.tryLock()) {
                    if ((ae.state()&STATE_MASK) == CREATED && ae.nReceivers == 0) {
                        try {
                            for (int i = 0, n = ae.nProducers; i < n; i++)
                                ((Producer<B>)ae.producer(i)).forceRegisterOn(this);
                            return;
                        } finally {
                            ae.unlock(ae.statePlain(), STATE_MASK, CANCELLED_DELIVERED);
                        }
                    } else {
                        ae.unlock(ae.statePlain());
                    }
                }
                sameType = (Emitter<B>) up;
            }
            if (proj != null)
                sameType = proj.subscribeTo(sameType);
            registerProducer(new ReceiverProducer<>(sameType, this));
        }
    }

    /**
     * Notifies that one of the producers feeding this {@link AsyncEmitter} has completed,
     * failed or got cancelled.
     *
     * <p>This method is idempotent, but SHOULD be called once per producer. Once all producers
     * terminate the {@link AsyncEmitter} itself will terminate. If at least one producer
     * terminated on error, the {@link AsyncEmitter} will terminate with that error. Else, if at
     * least one producer terminated by cancellation, then the {@link AsyncEmitter} will terminate
     * by cancellation. Finally, if all producers terminated with normal completion, then the
     * {@link AsyncEmitter} will also complete normally.</p>
     */
    public void producerTerminated() {
        if (THREAD_JOURNAL)
            journal("prodTerm st=", state(), "termProds=", (int)TERM_PRODS.getOpaque(this));
        if ((state() & (IS_TERM|IS_PENDING_TERM)) != 0)
            return; // ignore calls after the AsyncEmitter itself terminates
        if ((int)TERM_PRODS.getAndAddRelease(this, 1)+1 < nProducers)
            return; // some producers are not yet terminated, don't bother checking
        boolean cancelled = false;
        Throwable firstError = null, error;
        for (int i = 0, n = nProducers; i < n; i++) {
            var p = producer(i);
            if (p.isCancelled()) {
                cancelled = true;
            } else if ((error = p.error()) != null) {
                if (firstError == null) firstError = error;
            } else if (!p.isComplete()) {
                return;
            }
        }
        int tgt;
        if (firstError != null){
            if (this.error == UNSET_ERROR) this.error = firstError;
            tgt = PENDING_FAILED;
        } else if (cancelled) {
            tgt = PENDING_CANCELLED;
        } else {
            tgt = PENDING_COMPLETED;
        }
        if (moveStateRelease(statePlain(), tgt)) {
            if (!runNow()) awake();
        }
    }

    /**
     * Peek at the current number of {@link #request(long)}ed rows.
     *
     * @return the current number of unsatisfied {@link #request(long)}ed rows. <strong>May be
     *         negative</strong>
     */
    public long requested() { return (long)REQUESTED.getAcquire(this); }

    @Override public @Nullable B offer(B b) throws CancelledException, TerminatedException {
        if (b == null || b.rows == 0) return b;
        REQUESTED.getAndAddRelease(this, -b.rows);
        int state = lock(statePlain());
        try {
            while (true) {
                if ((state&STATE_MASK) == CANCEL_REQUESTED) {
                    throw CancelledException.INSTANCE; // caller retains ownership
                } else if ((state&IS_TERM) != 0) {
                    throw TerminatedException.INSTANCE;
                } else {
                    b.requireUnpooled();
                    if (filling == null) { // make b READY or filling
                        if (READY.compareAndExchangeRelease(this, null, b) != null)
                            filling = b;
                        b = null;
                        break;
                    } else if (READY.compareAndExchangeRelease(this, null, filling) == null) {
                        filling = b; //filling became READY, make b filling
                        b = null;
                        break;
                    } else if ((b.rows > 1 || !filling.fits(b)) && inEmitterService()) {
                        // avoid non-trivial and realloc filling.put() calls
                        unlock(state);
                        runNow();
                        state = lock(statePlain());
                    } else {
                        filling.put(b); // copy contents, caller retains ownership
                        break;
                    }
                }
            }
        } finally {
            unlock(state);
        }
        awake();
        if (b == null) {
            if ((b = (B)RECYCLED0.getAndSetRelease(this, null)) == null)
                b = (B)RECYCLED1.getAndSetRelease(this, null);
            if (b != null)
                b.unmarkPooled();
        }
        return b;
    }


    /* --- --- --- consumer methods --- --- --- */

    @Override public boolean canScatter() {
        return true;
    }

    @Override public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException {
        if (THREAD_JOURNAL) journal("subscribe", receiver, "on", this);
        int state = lockForRegister();
        try {
            if (EmitterStats.ENABLED && stats != null)
                ++stats.receivers;
            short n = nReceivers;
            if (n < PACKED_PEERS) RECV[n].set(this, receiver);
            else                  addExtraPeer(receiver, n, RECV_NUDGE);
            nReceivers = (short)(n+1);
            if (ThreadJournal.THREAD_JOURNAL)
                ThreadJournal.journal("subscribed", receiver, "to", this);
        } finally {
            unlock(state);
        }
    }

    @Override public void rebindAcquire() {
        super.rebindAcquire();
        for (int i = 0, n = Math.min(nProducers, PACKED_PEERS); i < n; i++) {
            Producer<?> p = (Producer<?>) PROD[i].get(this);
            try {
                p.rebindAcquire();
            } catch (Throwable t) {
                log.error("Failed to rebindAcquire() {}-th producer of {}, {}", i, this, p, t);
            }
        }
        if (nProducers > PACKED_PEERS)
            rebindAcquireNotPacked();
    }
    private void rebindAcquireNotPacked() {
        for (int i = PACKED_PEERS; i < nProducers; i++) {
            Producer<B> p = producer(i);
            try {
                p.rebindAcquire();
            } catch (Throwable t) {
                log.error("Failed to rebindAcquire() {}-th producer of {}, {}", i, this, p, t);
            }
        }
    }
    @Override public void rebindRelease() {
        super.rebindRelease();
        for (int i = 0, n = Math.min(nProducers, PACKED_PEERS); i < n; i++) {
            Producer<?> p = (Producer<?>) PROD[i].get(this);
            try {
                p.rebindRelease();
            } catch (Throwable t) {
                log.error("Failed to rebindRelease() {}-th producer of {}, {}", i, this, p, t);
            }
        }
        if (nProducers > PACKED_PEERS)
            rebindReleaseNotPacked();
    }
    private void rebindReleaseNotPacked() {
        for (int i = PACKED_PEERS; i < nProducers; i++) {
            Producer<B> p = producer(i);
            try {
                p.rebindRelease();
            } catch (Throwable t) {
                log.error("Failed to rebindRelease() {}-th producer of {}, {}", i, this, p, t);
            }
        }
    }

    @Override public void rebind(BatchBinding<B> binding) throws RebindException {
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        try {
            for (int i = 0, n = Math.min(nProducers, PACKED_PEERS); i < n; i++)
                ((Producer<B>) PROD[i].get(this)).rebind(binding);
            if (nProducers > PACKED_PEERS)
                rebindNotPacked(binding);
        } catch (Throwable t) {
            cancel();
            if (t instanceof RebindException e)
                throw e;
            throw new RebindException("Unexpected error rebinding producers", t);
        }
    }
    private void rebindNotPacked(BatchBinding<B> binding) {
        for (int i = PACKED_PEERS; i < nProducers; i++)
            producer(i).rebind(binding);
    }

    public static final class NoProducerException extends IllegalStateException {
        public NoProducerException() {
            super("No producer registered, request() will block registrations and will never be satisfied");
        }
    }

    @Override protected void onFirstRequest() {
        super.onFirstRequest();
        if (nReceivers == 0)
            throw new NoReceiverException();
        if (nProducers == 0)
            throw new NoProducerException();
    }

    @Override protected void resume() {
        for (int i = 0, n = Math.min(nProducers, PACKED_PEERS); i < n; i++) 
            ((Producer<B>) PROD[i].get(this)).resume();
        if (nProducers > PACKED_PEERS) {
            resumeNotPacked();
        }
    }
    private void resumeNotPacked() {
        for (int i = PACKED_PEERS; i < nProducers; i++)
            producer(i).resume();
    }

    private static final int TASK_SPINS = 16;

    @Override protected void task() {
        // read state as early as possible. If we observe the emitter terminated then it holds
        // that the emitter will not schedule more batches beyond what is already present at
        // READY and filling. Reading the state before polling the batches ensures that the
        // polled batches (if any) are certainly the last ones.
        int state = state();
        if ((state&IS_TERM_DELIVERED) != 0)
            return;
        if ((state&CANCEL_REQUESTING) != 0)
            state = doCancel(state);

        //deliver READY
        B b0 = (B)READY.getAndSetAcquire(this, null), b1;
        if (b0 != null) deliver(b0);

        // take READY and filling if we get the lock in a few spins
        boolean locked = false;
        for (int i = 0; i < TASK_SPINS && !(locked = tryLock()); i++)
            onSpinWait();
        if (!locked) {
            awake();
            return;// else: run other tasks instead of spinning
        }
        b0 = (B)READY.getAndSetAcquire(this, null);
        b1 = filling;
        filling = null;
        unlock(state);

        //deliver READY and filling
        if (b0 != null) deliver(b0);
        if (b1 != null) deliver(b1);

        //deliver termination
        if ((state&IS_PENDING_TERM) != 0)
            deliverTermination(state);
    }

    private int doCancel(int cancelState) {
        if (cancelState != CANCEL_REQUESTING)
            return cancelState;
        Throwable error = null;
        int fails = 0;
        for (int i = 0; i < nProducers; i++) {
            try {
                producer(i).cancel();
            } catch (Throwable t) {
                ++fails;
                if (error == null)  error = t;
            }
        }
        moveStateRelease(statePlain(), CANCEL_REQUESTED);
        if (error != null) {
            log.warn("{}: Ignoring failure to cancel() {}/{} producers",
                     this, fails, nProducers, error);
        }
        return (cancelState&FLAGS_MASK) | CANCEL_REQUESTED;
    }

    private void deliver(B b) {
        int last = nReceivers-1;
        B offer;
        if (last == 0) {
            offer = deliver(receiver0, b);
        } else if (last < PACKED_PEERS) {
            B copyOffer = scatterRecycled;
            scatterRecycled = null;
            if (copyOffer != null)
                copyOffer.unmarkPooled();
            switch (last) {
                case 11: copyOffer = deliverCopy(receiverB, b, copyOffer);
                case 10: copyOffer = deliverCopy(receiverA, b, copyOffer);
                case  9: copyOffer = deliverCopy(receiver9, b, copyOffer);
                case  8: copyOffer = deliverCopy(receiver8, b, copyOffer);
                case  7: copyOffer = deliverCopy(receiver7, b, copyOffer);
                case  6: copyOffer = deliverCopy(receiver6, b, copyOffer);
                case  5: copyOffer = deliverCopy(receiver5, b, copyOffer);
                case  4: copyOffer = deliverCopy(receiver4, b, copyOffer);
                case  3: copyOffer = deliverCopy(receiver3, b, copyOffer);
                case  2: copyOffer = deliverCopy(receiver2, b, copyOffer);
                case  1: copyOffer = deliverCopy(receiver1, b, copyOffer);
                         offer     = deliver(receiver0, b);
                         break;
                default: throw new AssertionError("unreachable code");
            }
            if (copyOffer != null) {
                copyOffer.markPooled();
                scatterRecycled = copyOffer;
            }
        } else {
            offer = wideDeliver(b);
        }
        if (offer != null) {
            offer.markPooled();
            if (RECYCLED0.compareAndExchangeRelease(this, null, offer) != null) {
                if (RECYCLED1.compareAndExchangeRelease(this, null, offer) != null) {
                    offer.unmarkPooled();
                    offer.recycle();
                }
            }
        }
    }
    private @Nullable B deliverCopy(Receiver<B> r, B b, @Nullable B copyOffer) {
        return deliver(r, b.copy(copyOffer));
    }

    private B wideDeliver(B b) {
        B copyOffer = scatterRecycled;
        for (VarHandle recv : RECV)
            copyOffer = deliver((Receiver<B>)recv.get(this), b.copy(copyOffer));
        for (int i = RECV_NUDGE, n = ((nReceivers-PACKED_PEERS)<<1) + RECV_NUDGE; i < n; i += 2)
            copyOffer = deliver((Receiver<B>)extraPeers[i], b.copy(copyOffer));
        return b;
    }

    private void deliverTermination(int pending) {
        if (markNotPending(pending)) {
            int term = (pending&~IS_PENDING_TERM)|IS_TERM;
            for (int i = 0, n = Math.min(PACKED_PEERS, nReceivers); i < n; i++)
                deliverTermination((Receiver<B>)RECV[i].get(this), term);
            if (nReceivers > PACKED_PEERS)
                deliverTerminationExtra(term);
            markDelivered(term);
        }
    }

    private void deliverTerminationExtra(int terminated) {
        for (int i = RECV_NUDGE, n = (nReceivers-PACKED_PEERS)<<1; i < n; i += 2)
            deliverTermination((Receiver<B>)extraPeers[i], terminated);
    }

}
