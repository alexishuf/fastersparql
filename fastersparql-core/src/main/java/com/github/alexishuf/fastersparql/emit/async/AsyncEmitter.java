package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
public final class AsyncEmitter<B extends Batch<B>>
        extends RecurringTaskRunner.Task implements Emitter<B> {
    private static final Logger log = LoggerFactory.getLogger(AsyncEmitter.class);
    private static final boolean IS_DEBUG = log.isDebugEnabled();

    private static final VarHandle READY, FILL_LOCK, S, REQUESTED, TERM_PRODS;
    private static final VarHandle RECYCLED0, RECYCLED1;
    private static final VarHandle NEXT_ID;
    static {
        try {
            READY = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainReady", Batch.class);
            REQUESTED = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRequested", long.class);
            TERM_PRODS = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainTerminatedProducers", int.class);
            RECYCLED0 = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRecycled0", Batch.class);
            RECYCLED1 = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainRecycled1", Batch.class);
            FILL_LOCK = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainFillLock", int.class);
            S = MethodHandles.lookup().findVarHandle(AsyncEmitter.class, "plainState", State.class);
            NEXT_ID = MethodHandles.lookup().findStaticVarHandle(AsyncEmitter.class, "plainNextId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
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

    @SuppressWarnings("unused") private B plainReady, plainRecycled0, plainRecycled1;
    @SuppressWarnings("unused") private int plainFillLock;
    @SuppressWarnings({"unused", "FieldMayBeFinal"}) private State plainState = State.CREATED;
    @SuppressWarnings("unused") private long plainRequested;
    @SuppressWarnings("unused") private int plainTerminatedProducers;
    private B filling;
    private @Nullable Throwable error;
    private int id;
    public final Vars vars;
    private final BatchType<B> batchType;
    private int nReceivers, nProducers;
    private @Nullable Receiver<B> receiver;
    private Receiver<B> @Nullable[] receivers;
    private @Nullable Producer producer;
    private Producer @Nullable[] producers;

    public AsyncEmitter(BatchType<B> batchType, Vars vars, RecurringTaskRunner runner) {
        super(runner);
        this.batchType = batchType;
        this.vars = vars;
    }

    /* --- --- --- java.lang.Object --- --- --- */

    @Override public String toString() {
        if (id == 0)
            id = (int)NEXT_ID.getAndAddRelease(1)+1;
        return getClass().getSimpleName()+"@"+id+"["+ S.getOpaque(this)+"]";
    }

    /* --- --- --- internals shared by producer and consumer --- --- --- */

    @SuppressWarnings("unchecked") private B takeRecycled() {
        B b = (B)RECYCLED0.getAndSetAcquire(this, null);
        if (b == null)
            b = (B)RECYCLED1.getAndSetAcquire(this, null);
        if (b != null)
            b.unmarkPooled();
        return b;
    }

    private void recycle(@Nullable B b) {
        if (b == null) return;
        b.markPooled();
        if (RECYCLED0.compareAndExchangeRelease(this, null, b) == null) return;
        if (RECYCLED1.compareAndExchangeRelease(this, null, b) == null) return;
        b.unmarkPooled();
        batchType.recycle(b);
    }

    private void acquireRegistering() {
        while (true) {
            var s = (State) S.compareAndExchangeAcquire(this, State.CREATED, State.REGISTERING);
            if      (s == State.REGISTERING) onSpinWait();
            else if (s == State.CREATED    ) break;
            else                             throw new RegisterAfterStartException();
        }
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
            if (this.nProducers == 0) {
                this.producer = producer;
            } else {
                if (producers == null)
                    (producers = new Producer[10])[0] = this.producer;
                else if (nProducers == producers.length)
                    producers = Arrays.copyOf(producers, nProducers+(nProducers>>1));
                producers[nProducers] = producer;
            }
            ++nProducers;
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
        int nTerm = (int)TERM_PRODS.getAndAddRelease(this, 1)+1;
        if (nTerm < nProducers) return;
        boolean cancelled = false, terminated = true;
        Throwable firstError = null, error;
        if (producers != null) {
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
        } else if (producer != null) {
            cancelled = producer.isCancelled();
            firstError = producer.error();
            terminated = producer.isTerminated();
        }
        if (terminated) {
            State ex, tgt, s = plainState;
            if (firstError != null) {
                tgt = State.FAILED;
                this.error = firstError;
            } else {
                tgt = cancelled ? State.CANCEL_COMPLETED : State.COMPLETED;
            }
            while ((s = (State) S.compareAndExchangeRelease(this, ex=s, tgt)) != ex) {
                if (s.isTerminated()) return;
                onSpinWait();
            }
            awake(); // deliver termination
        }
    }

    /**
     * Peek at the current number of {@link #request(long)}ed rows.
     *
     * @return the current number of unsatisfied {@link #request(long)}ed rows. <strong>May be
     *         negative</strong>
     */
    public long requested() { return (long)REQUESTED.getAcquire(this); }

    /**
     * Sends {@code b} or {@code b} rows to the receiver of this {@link AsyncEmitter}.
     *
     * @param b the batch that may be passed on or may be copied from
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
        } else if (READY.compareAndExchangeRelease(this, null, b) == null) {
            b = null; // b passed to READY, caller lost ownership
        } else {
            while ((int)FILL_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) onSpinWait();
            try {
                b.requireUnpooled();
                if (filling == null) {
                    if (READY.compareAndExchangeRelease(this, null, b) != null)
                        filling =  b; // could make b READY, use as filling
                    b = null; // b passed to READY or filling, caller lost ownership
                } else {
                    filling.put(b); // copy contents, caller retains ownership
                }
            } finally {
                FILL_LOCK.setRelease(this, 0);
            }
        }
        awake();
        return b == null ? takeRecycled() : b;
    }


    /* --- --- --- consumer methods --- --- --- */

    @Override public Vars vars() { return vars; }

    @Override public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException {
        acquireRegistering();
        try {
            if (this.nReceivers == 0) {
                this.receiver = receiver;
            } else {
                if (receivers == null) //noinspection unchecked
                    (receivers = new Receiver[10])[0] = this.receiver;
                else if (nReceivers == receivers.length)
                    receivers = Arrays.copyOf(receivers, nReceivers+(nReceivers>>1));
                receivers[nReceivers] = receiver;
            }
            ++nReceivers;
        } finally {
            S.setRelease(this, State.CREATED);
        }
    }


    @Override public void cancel() {
        State tgt = State.CANCEL_REQUESTING;
        if ((State) S.compareAndExchangeAcquire(this, State.LIVE, tgt) == State.LIVE) {
            try {
                if (producers != null) {
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
                } else if (producer != null) {
                    producer.cancel();
                }
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
            if (producers != null) {
                for (int i = 0; i < nProducers; i++)
                    producers[i].resume();
            } else if (producer != null) {
                producer.resume();
            }
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

    @SuppressWarnings("unchecked") @Override protected RecurringTaskRunner.TaskResult task() {
        // read state as early as possible. If we observe the emitter terminated then it holds
        // that the emitter will not schedule more batches beyond what is already present at
        // READY and filling. Reading the state before polling the batches ensures that the
        // polled batches (if any) are certainly the last ones.
        State earlyState = (State) S.getVolatile(this);
        switch (earlyState) {
            case COMPLETED_DELIVERED, FAILED_DELIVERED, CANCEL_COMPLETED_DELIVERED, CREATED
                    -> { return RecurringTaskRunner.TaskResult.DONE; }
        }

        // acquire FILL_LOCK quickly or yield to another Task
        for (int i = 0; i < TASK_SPINS; i++) {
            if ((int)FILL_LOCK.compareAndExchangeAcquire(this, 0, 1) == 0)
                break; // acquired lock
            else if (i == TASK_LAST_SPIN)
                return RecurringTaskRunner.TaskResult.RESCHEDULE; //try other task
            onSpinWait(); //retry
        }

        // got FILL_LOCK, take READY and filling
        B b0, b1;
        try {
            b0 = (B)READY.getAndSetAcquire(this, null);
            b1 = filling;
            filling = null;
        } finally { FILL_LOCK.setRelease(this, 0); }

        // deliver READY, filling and poll READY once more AFTER 2 deliveries
        if (receivers != null) {
            deliver(receivers, b0, b1);
        } else {
            Receiver<B> r = this.receiver;
            if ( b0                                          != null) deliver(r, b0);
            if ( b1                                          != null) deliver(r, b1);
            if ((b0 = (B)READY.getAndSetAcquire(this, null)) != null) deliver(r, b0);
        }

        // deliver termination event
        switch (earlyState) {
            case CANCEL_COMPLETED,COMPLETED,FAILED -> deliverTermination(earlyState);
        }
        return RecurringTaskRunner.TaskResult.DONE;
    }

    @SuppressWarnings("unchecked")
    private void deliver(Receiver<B>[] receivers, B b0, B b1) {
        int n = this.nReceivers;
        if ( b0                                          != null) deliver(receivers, n, b0);
        if ( b1                                          != null) deliver(receivers, n, b1);
        if ((b0 = (B)READY.getAndSetAcquire(this, null)) != null) deliver(receivers, n, b0);
    }

    private void deliver(Receiver<B>[] rs, int n, B b) {
        for (int i = 0, last = n-1; i < n; i++) {
            B copy = i == last ? b : b.copy(takeRecycled());
            deliver(rs[i], copy);
        }
    }

    private void deliver(Receiver<B> receiver, B b) {
        try {
            b = receiver.onBatch(b);
        } catch (Throwable t) {
            handleEmitError(receiver, t);
        }
        recycle(b);
    }

    private void deliverTermination(State terminated) {
        var tgt = terminated.asDelivered();
        if ((State) S.compareAndExchangeRelease(this, terminated, tgt) != terminated) {
            assert false : "undelivered terminated state changed";
            return;
        }
        if (receivers != null) {
            for (int i = 0; i < nReceivers; i++)
                deliverTermination(terminated, receivers[i]);
        } else {
            assert receiver != null;
            deliverTermination(terminated, receiver);
        }
        for (B b; (b = takeRecycled()) != null; )
            batchType.recycle(b);
        unload();
    }

    private void deliverTermination(State terminated, Receiver<B> r) {
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

    private void handleTerminationError(Receiver<B> receiver, Throwable e) {
        String name = e.getClass().getSimpleName();
        if (IS_DEBUG)
            log.info("Ignoring {} while delivering termiantion to {}", name, receiver, e);
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
