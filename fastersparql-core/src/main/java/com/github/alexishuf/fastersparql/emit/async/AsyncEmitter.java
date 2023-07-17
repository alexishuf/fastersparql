package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.AffinityPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.util.ExceptionCondenser.throwAsUnchecked;
import static java.lang.Thread.onSpinWait;

/**
 * An emitter that feeds 1+ {@link Receiver}s from 1+ {@link Producer}s,
 * decoupling time coupling of both sets: producers can produce concurrently with
 * receivers consumption.
 *
 * @param <B> the batch type
 */
@SuppressWarnings("unchecked")
public final class AsyncEmitter<B extends Batch<B>>
        extends RecurringTaskRunner.Task implements Emitter<B> {
    private static final Logger log = LoggerFactory.getLogger(AsyncEmitter.class);
    private static final boolean IS_DEBUG = log.isDebugEnabled();
    private static final int ARRAY_INIT_CAP = 32;
    private static final int POOL_CAP       = 256;
    private static final int POOL_THREADS   = Runtime.getRuntime().availableProcessors();
    @SuppressWarnings("RedundantCast")
    private static final AffinityPool<Receiver<?>[]> RECV_POOL
            = new AffinityPool<>((Class<Receiver<?>[]>) (Object)Receiver[].class, POOL_CAP, POOL_THREADS);
    private static final AffinityPool<Producer[]>    PROD_POOL
            = new AffinityPool<>(Producer[].class, POOL_CAP, POOL_THREADS);
    private static final Receiver<?>[] NO_RECEIVERS = new Receiver[0];
    private static final Producer[] NO_PRODUCERS = new Producer[0];

    private static final VarHandle READY, FILL_LOCK, S, REQUESTED, TERM_PRODS;
    private static final VarHandle RECYCLED0, RECYCLED1;
    private static final VarHandle NEXT_ID;
    static {
        try {
            READY      = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainReady", Batch.class);
            RECYCLED0  = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRecycled0", Batch.class);
            RECYCLED1  = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRecycled1", Batch.class);
            REQUESTED  = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRequested", long.class);
            TERM_PRODS = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainTerminatedProducers", int.class);
            FILL_LOCK  = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainFillLock", int.class);
            S          = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainState", State.class);
            NEXT_ID    = MethodHandles.lookup().findStaticVarHandle(AsyncEmitter.class, "plainNextId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
        for (int i = 0, n = POOL_CAP/4; i < n; i++) {
            RECV_POOL.offer(new Receiver[ARRAY_INIT_CAP]);
            PROD_POOL.offer(new Producer[ARRAY_INIT_CAP]);
        }
    }

    @SuppressWarnings("unused") private static int plainNextId;

    private enum State {
        CREATED,
        REGISTERING,
        LIVE,
        COMPLETED,
        FAILED,
        CANCEL_REQUESTING,
        CANCEL_REQUESTED,
        CANCEL_COMPLETED,
        COMPLETED_DELIVERED,
        FAILED_DELIVERED,
        CANCEL_COMPLETED_DELIVERED;

        State asDelivered() {
            return switch (this) {
                case COMPLETED        -> COMPLETED_DELIVERED;
                case FAILED           -> FAILED_DELIVERED;
                case CANCEL_COMPLETED -> CANCEL_COMPLETED_DELIVERED;
                default -> throw new UnsupportedOperationException();
            };
        }
        boolean isTerminated() {
            return switch (this) {
                case COMPLETED,COMPLETED_DELIVERED,
                     CANCEL_COMPLETED,CANCEL_COMPLETED_DELIVERED,
                     FAILED, FAILED_DELIVERED -> true;
                default -> false;
            };
        }
    }

    @SuppressWarnings("unused") private B plainReady;
    @SuppressWarnings("unused") private int plainFillLock;
    @SuppressWarnings({"unused", "FieldMayBeFinal"}) private State plainState = State.CREATED;
    @SuppressWarnings("unused") private long plainRequested;
    @SuppressWarnings("unused") private int plainTerminatedProducers;
    @SuppressWarnings("unused") private @Nullable B plainRecycled0, plainRecycled1;
    private @Nullable B scatterRecycled, filling;
    private @Nullable Throwable error;
    private int id;
    public final Vars vars;
    private int nReceivers, nProducers;
    private Receiver<B>[] receivers;
    private Producer[] producers;

    public AsyncEmitter(Vars vars, RecurringTaskRunner runner) {
        super(runner);
        this.vars = vars;
        Receiver<B>[] receivers = (Receiver<B>[])RECV_POOL.get();
        if (receivers == null)
            receivers = (Receiver<B>[]) Array.newInstance(Receiver.class, ARRAY_INIT_CAP);
        Producer[] producers = PROD_POOL.get();
        if (producers == null)
            producers = (Producer[])Array.newInstance(Producer.class, ARRAY_INIT_CAP);
        this.receivers = receivers;
        this.producers = producers;
    }

    /* --- --- --- java.lang.Object --- --- --- */

    @Override public String toString() {
        if (id == 0)
            id = (int)NEXT_ID.getAndAddRelease(1)+1;
        return getClass().getSimpleName()+"@"+id+"["+ S.getOpaque(this)+"]";
    }

    /* --- --- --- internals shared by producer and consumer --- --- --- */

    private void acquireRegistering() {
        while (true) {
            var s = (State) S.compareAndExchangeAcquire(this, State.CREATED, State.REGISTERING);
            if      (s == State.REGISTERING) onSpinWait();
            else if (s == State.CREATED    ) break;
            else                             throw new RegisterAfterStartException();
        }
    }

    private static <T> T[] grow(T[] array, AffinityPool<T[]> pool) {
        T[] bigger = Arrays.copyOf(array, array.length + (array.length >> 1));
        Arrays.fill(array, null);
        pool.offer(array);
        return bigger;
    }

    /* --- --- --- producer methods --- --- --- */

    public static sealed class AsyncEmitterStateException extends Exception
            permits CancelledException, TerminatedException {
        public AsyncEmitterStateException(String message) { super(message); }
    }

    public static final class CancelledException extends AsyncEmitterStateException {
        public static final CancelledException INSTANCE = new CancelledException();
        private CancelledException() {super("AsyncEmitter cancel()ed, cannot offer/copy");}
    }

    public static final class TerminatedException extends AsyncEmitterStateException {
        public static final TerminatedException INSTANCE = new TerminatedException();
        private TerminatedException() {
            super("AsyncEmitter cancelled/completed/failed by the producer");
        }
    }

    /**
     * Register a {@link Producer} that will feed this {@link AsyncEmitter} via
     * {@link #offer(Batch)}.
     *
     * @param producer the Producer instance.
     * @throws RegisterAfterStartException if this called after the first {@link #request(long)}.
     */
    public void registerProducer(Producer producer) throws RegisterAfterStartException {
        acquireRegistering();
        try {
            int n = this.nProducers;
            Producer[] a = this.producers;
            if (n == a.length)
                this.producers = a = grow(a, PROD_POOL);
            a[n] = producer;
            this.nProducers = n+1;
        } finally {
            S.setRelease(this, State.CREATED);
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
        int nTerm = (int)TERM_PRODS.getAndAdd(this, 1)+1;
        if (nTerm < nProducers)
            return;
        if (plainState.isTerminated())
            return;
        boolean cancelled = false, terminated = true;
        Throwable firstError = null, error;
        for (int i = 0; i < nProducers; i++) {
            var p = producers[i];
            if (p.isCancelled()) {
                cancelled = true;
            } else if ((error = p.error()) != null) {
                if (firstError == null)
                    firstError = error;
            } else if (!p.isComplete()) {
                terminated = false;
                break;
            }
        }
        if (terminated) {
            State ex, tgt, curr = plainState;
            if (firstError != null) {
                tgt = State.FAILED;
                this.error = firstError;
            } else {
                tgt = cancelled ? State.CANCEL_COMPLETED : State.COMPLETED;
            }
            do {
                curr = (State)S.compareAndExchangeRelease(this, ex=curr, tgt);
                if (curr.isTerminated()) return;
            } while (curr != ex);
            awake(); // deliver termination
        }
    }

    /**
     * Peek at the current number of {@link #request(long)}ed rows.
     *
     * @return the current number of unsatisfied {@link #request(long)}ed rows. <strong>May be
     *         negative</strong>
     */
    public long requested() { return (long)REQUESTED.getOpaque(this); }

    /**
     * Sends {@code b} or {@code b} rows to the receiver of this {@link AsyncEmitter}.
     *
     * @param b the batch that may be passed on or may be copied from
     *                 making this offer.
     * @return {@code null} or a (likely non-empty) batch to replace {@code b} if ownership of
     *         {@code b} has been taken by {@link AsyncEmitter} or by its receiver. {@code b}
     *         itself may be returned if its contents were copied elsewhere, and thus ownership
     *         was retained by the caller.
     * @throws CancelledException if {@link #cancel()} was previously called. If this is
     *                            raised, ownership of {@code b} remains with the caller and its
     *                            contents will not be delivered to the receiver.
     */
    public @Nullable B offer(B b) throws CancelledException, TerminatedException {
        if (b == null || b.rows == 0) return b;
        REQUESTED.getAndAddRelease(this, -b.rows);
        b.requireUnpooled();
        State state = (State) S.getAcquire(this);
        if (state == State.CANCEL_REQUESTED) {
            throw CancelledException.INSTANCE; // caller retains ownership
        } else if (state.isTerminated()) {
            throw TerminatedException.INSTANCE;
        } else {
            while ((int)FILL_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                b.requireUnpooled();
                if (filling == null) { // make b READY or filling
                    if (READY.compareAndExchangeRelease(this, null, b) != null)
                        filling =  b;
                    b = null;
                } else if (READY.compareAndExchangeRelease(this, null, filling) == null) {
                    filling = b; //filling became READY, make b filling
                    b = null;
                } else {
                    filling.put(b); // copy contents, caller retains ownership
                }
            } finally {
                FILL_LOCK.setRelease(this, 0);
            }
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

    @Override public Vars vars() { return vars; }

    @Override public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException {
        acquireRegistering();
        try {
            int n = this.nReceivers;
            var a = this.receivers;
            if (n == a.length)
                this.receivers = a = (Receiver<B>[]) grow(a, RECV_POOL);
            a[n] = receiver;
            this.nReceivers = n+1;
        } finally {
            S.setRelease(this, State.CREATED);
        }
    }


    @Override public void cancel() {
        State tgt = State.CANCEL_REQUESTING;
        if ((State) S.compareAndExchangeAcquire(this, State.LIVE, tgt) == State.LIVE) {
            try {
                Throwable error = null;
                for (int i = 0; i < nProducers; i++) {
                    try {
                        producers[i].cancel();
                    } catch (Throwable t) {
                        if (error == null)  error = t;
                        else                error.addSuppressed(t);
                    }
                }
                if (error != null)
                    throwAsUnchecked(error);
            } finally {
                S.compareAndExchangeRelease(this, tgt, State.CANCEL_REQUESTED);
            }
        }
    }

    public static final class NoProducerException extends IllegalStateException {
        public NoProducerException() {
            super("No producer registered, request() will block registrations and will never be satisfied");
        }
    }

    @Override public void request(long rows) throws NoReceiverException, NoProducerException {
        if (rows <= 0)
            return;
        // on first request(), transition from CREATED to LIVE
        if (plainState == State.CREATED || plainState == State.REGISTERING)
            onFirstRequest();

        // add rows to REQUESTED protecting against overflow
        long old = plainRequested, next, ex;
        do {
            next = old + rows;
            if (old > 0 && next < 0) // overflow detected
                next = Long.MAX_VALUE;
        } while ((old=(long) REQUESTED.compareAndExchangeRelease(this, ex=old, next)) != ex);

        // (re)start producers, if needed and allowed
        if (old <= 0 && next > 0) {
            for (int i = 0; i < nProducers; i++)
                producers[i].resume();
        }
    }

    private void onFirstRequest() {
        while ((State) S.compareAndExchange(this, State.CREATED, State.LIVE) == State.REGISTERING)
            onSpinWait();
        if (nReceivers == 0)
            throw new NoReceiverException();
        if (nProducers == 0)
            throw new NoProducerException();
    }

    private static final int TASK_SPINS     = 32;
    private static final int TASK_LAST_SPIN = TASK_SPINS-1;

    @Override protected RecurringTaskRunner.TaskResult task() {
        // read state as early as possible. If we observe the emitter terminated then it holds
        // that the emitter will not schedule more batches beyond what is already present at
        // READY and filling. Reading the state before polling the batches ensures that the
        // polled batches (if any) are certainly the last ones.
        State earlyState = (State) S.getVolatile(this);
        switch (earlyState) {
            case COMPLETED_DELIVERED, FAILED_DELIVERED, CANCEL_COMPLETED_DELIVERED, CREATED
                    -> { return RecurringTaskRunner.TaskResult.DONE; }
        }

        // deliver READY
        var receivers = this.receivers;
        var last = this.nReceivers-1;
        B b0 = (B)READY.getAndSetAcquire(this, null), b1;
        if (b0 != null) deliver(receivers, last, b0);

        // acquire FILL_LOCK quickly or yield to another Task
        for (int i = 0; i < TASK_SPINS; i++) {
            if ((int)FILL_LOCK.compareAndExchangeAcquire(this, 0, 1) == 0)
                break; // acquired lock
            else if (i == TASK_LAST_SPIN)
                return RecurringTaskRunner.TaskResult.RESCHEDULE; //try other task
            onSpinWait(); //retry
        }
        // got FILL_LOCK, take READY and filling
        b0 = (B)READY.getAndSetAcquire(this, null);
        b1 = filling;
        filling = null;
        FILL_LOCK.setRelease(this, 0);

        // deliver READY and filling
        if (b0 != null) deliver(receivers, last, b0);
        if (b1 != null) deliver(receivers, last, b1);

        // deliver termination event
        switch (earlyState) {
            case CANCEL_COMPLETED,COMPLETED,FAILED -> deliverTermination(earlyState);
        }
        return RecurringTaskRunner.TaskResult.DONE;
    }

    private void deliver(Receiver<B>[] receivers, int last, @NonNull B b) {
        if (last == 0) {
            try {
                B offer = receivers[0].onBatch(b);
                if (offer != null) {
                    offer.markPooled();
                    if (RECYCLED0.compareAndExchangeRelease(this, null, offer) != null) {
                        if (RECYCLED1.compareAndExchangeRelease(this, null, offer) != null) {
                            offer.unmarkPooled();
                            offer.recycle();
                        }
                    }
                }
            } catch (Throwable t) {
                handleEmitError(receivers[0], t);
            }
        } else {
            B offer = scatterRecycled;
            scatterRecycled = null;
            for (int i = 0; i <= last; i++) {
                B copy;
                if (i == last) {
                    copy = b;
                } else {
                    copy = b.copy(offer);
                }
                try {
                    offer = receivers[i].onBatch(copy);
                } catch (Throwable t) {
                    handleEmitError(receivers[i], t);
                }
            }
            scatterRecycled = offer;
        }
    }

    private void deliverTermination(State terminated) {
        var tgt = terminated.asDelivered();
        if ((State) S.compareAndExchangeRelease(this, terminated, tgt) != terminated) {
            assert false : "undelivered terminated state changed";
            return;
        }
        Receiver<B>[] receivers = this.receivers;
        for (int i = 0, n = nReceivers; i < n; i++) {
            Receiver<B> r = receivers[i];
            try {
                switch (terminated) {
                    case COMPLETED        -> r.onComplete();
                    case CANCEL_COMPLETED -> r.onCancelled();
                    case FAILED           -> r.onError(error);
                }
            } catch (Throwable t) {
                handleTerminationError(r, t);
            }
        }
        unload();
        nReceivers = nProducers = 0;
        this.receivers = (Receiver<B>[]) NO_RECEIVERS;
        this.producers = NO_PRODUCERS;
    }

    private void handleTerminationError(Receiver<B> receiver, Throwable e) {
        String name = e.getClass().getSimpleName();
        if (IS_DEBUG)
            log.info("Ignoring {} while delivering termination to {}", name, receiver, e);
        else
            log.info("Ignoring {} while delivering termination to {}", name, receiver);
    }

    private void handleEmitError(Receiver<B> receiver, Throwable emitError) {
        if (S.getOpaque(this) != State.LIVE) {
            log.debug("{}.onBatch() failed, will not cancel {}: terminated or terminating",
                      receiver, this, emitError);
        } else {
            log.error("{}.onBatch() failed, cancelling", receiver, emitError);
            try {
                cancel();
            } catch (Throwable cancelError) {
                log.info("Ignoring {}.cancel() failure", this, cancelError);
            }
        }
    }
}
