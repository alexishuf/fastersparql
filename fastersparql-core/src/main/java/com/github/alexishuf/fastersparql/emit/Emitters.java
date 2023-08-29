package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.async.*;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.THREAD_JOURNAL;
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

    public static <B extends Batch<B>> AsyncEmitter<B>
    fromProducer(BatchType<B> bt, Vars vars, Producer<B> producer) {
        AsyncEmitter<B> ae = new AsyncEmitter<>(bt, vars);
        producer.registerOn(ae);
        return ae;
    }

    public static <B extends Batch<B>> AsyncEmitter<B> fromBIt(BIt<B> it) {
        var ae = new AsyncEmitter<>(it.batchType(), it.vars());
        new BItProducer<>(it, ae);
        return ae;
    }

    public static <B extends Batch<B>> Emitter<B> withVars(Vars vars, Emitter<B> in) {
        var projector = in.batchType().projector(vars, in.vars());
        return projector == null ? in : projector.subscribeTo(in);
    }

    private static final class Collector<B extends Batch<B>> extends ReceiverFuture<B, B> {
        private B collected;

        @Override
        public @This Collector<B> subscribeTo(Emitter<B> e) throws RegisterAfterStartException {
            super.subscribeTo(e);
            collected = e.batchType().create(64, e.vars().size(), 0);
            return this;
        }

        @Override public B onBatch(B batch) {
            collected.put(batch);
            return batch;
        }
        @Override public void onComplete() { complete(collected); }
    }

    @SuppressWarnings("unused") public static <B extends Batch<B>> B collect(Emitter<B> e) {
        return new Collector<B>().subscribeTo(e).join();
    }

    public static void handleEmitError(Receiver<?> downstream, Emitter<?> upstream,
                                       boolean emitterTerminated,
                                       Throwable emitError) {
        if (THREAD_JOURNAL) journal("deliver failed, rcv=", downstream, ", on", upstream);
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
        if (THREAD_JOURNAL) journal("handleTerminationError, rcv=", downstream, ", on", upstream);
        String name = error.getClass().getSimpleName();
        if (LOG_DEBUG)
            log.info("Ignoring {} while delivering termination to {}", name, downstream, error);
        else
            log.info("Ignoring {} while delivering termination to {}", name, downstream);
    }


}
