package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.async.BatchEmitter;
import com.github.alexishuf.fastersparql.emit.async.EmptyEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.model.Vars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class Emitters {
    private static final Logger log = LoggerFactory.getLogger(Emitters.class);
    private static final boolean LOG_DEBUG = log.isDebugEnabled();

    public static <B extends Batch<B>> EmptyEmitter<B> empty(BatchType<B> batchType, Vars vars) {
        return new EmptyEmitter<>(batchType, vars, null);
    }

    @SuppressWarnings("unused")
    public static <B extends Batch<B>> EmptyEmitter<B> error(BatchType<B> batchType, Vars vars, Throwable error) {
        return new EmptyEmitter<>(batchType, vars, error);
    }

    public static <B extends Batch<B>> BatchEmitter<B> ofBatch(Vars vars, B batch) {
        return new BatchEmitter<>(vars, batch);
    }

    public static <B extends Batch<B>> Emitter<B> withVars(Vars vars, Emitter<B> in) {
        var projector = in.batchType().projector(vars, in.vars());
        return projector == null ? in : projector.subscribeTo(in);
    }

    public static <B extends Batch<B>> void discard(Emitter<B> em) {
        if (em == null)
            return;
        try {
            em.subscribe(new DiscardingReceiver<>());
        } catch (MultipleRegistrationUnsupportedException ignored) {}
        try {
            em.cancel();
        } catch (Throwable t) {
            log.error("Ignoring {} while discarding {}", t.getClass().getSimpleName(), em, t);
        }
    }

    @SuppressWarnings("unused") public static <B extends Batch<B>> B collect(Emitter<B> e) {
        return new CollectingReceiver<>(e).join();
    }

    public static <B extends Batch<B>> boolean ask(Emitter<B> e) {
        return new AskReceiver<>(e).has();
    }

    public static void handleEmitError(Receiver<?> downstream, Emitter<?> upstream,
                                       boolean emitterTerminated,
                                       Throwable emitError) {
        if (ENABLED) journal("deliver failed, rcv=", downstream, ", on", upstream);
        if (emitterTerminated) {
            log.debug("{}.onBatch() failed, will not cancel {}: terminated or terminating",
                    downstream, upstream, emitError);
        } else {
            log.error("{}.onBatch() failed, cancelling", downstream, emitError);
            try {
                upstream.cancel();
            } catch (Throwable cancelError) {
                log.info("Ignoring {}.cancel() failure", upstream, cancelError);
            }
        }
    }

    public static void handleTerminationError(Receiver<?> downstream, Emitter<?> upstream,
                                              Throwable error) {
        if (ENABLED) journal("handleTerminationError, rcv=", downstream, ", on", upstream);
        String name = error.getClass().getSimpleName();
        if (LOG_DEBUG)
            log.info("Ignoring {} while delivering termination to {}", name, downstream, error);
        else
            log.info("Ignoring {} while delivering termination to {}", name, downstream);
    }


}
