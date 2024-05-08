package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class Emitters {
    private static final Logger log = LoggerFactory.getLogger(Emitters.class);
    private static final boolean LOG_DEBUG = log.isDebugEnabled();

    public static <B extends Batch<B>> Orphan<EmptyEmitter<B>> empty(BatchType<B> batchType, Vars vars) {
        return EmptyEmitter.create(batchType, vars, null);
    }

    @SuppressWarnings("unused")
    public static <B extends Batch<B>> Orphan<EmptyEmitter<B>> error(BatchType<B> batchType, Vars vars, Throwable error) {
        return EmptyEmitter.create(batchType, vars, error);
    }

    public static <B extends Batch<B>> Orphan<BatchEmitter<B>> ofBatch(Vars vars, Orphan<B> batch) {
        return BatchEmitter.create(vars, batch);
    }

    public static <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    withVars(Vars vars, Orphan<? extends Emitter<B, ?>> in) {
        var orphanProjector = Emitter.peekBatchType(in).projector(vars, Emitter.peekVars(in));
        if (orphanProjector == null) return in;
        return orphanProjector.takeOwnership(WITH_VARS).subscribeTo(in).releaseOwnership(WITH_VARS);
    }
    private static final StaticMethodOwner WITH_VARS = new StaticMethodOwner("Emitters.withVars");

    public static <B extends Batch<B>> void discard(Orphan<? extends Emitter<B, ?>> em) {
        if (em == null)
            return;
        DiscardingReceiver<B> receiver = null;
        try {
            receiver = DiscardingReceiver.create(em).takeOwnership(DISCARD);
            receiver.cancel(true);
        } catch (Throwable t) {
            log.error("Ignoring {} while discarding {}", t.getClass().getSimpleName(), em, t);
        } finally {
            if (receiver != null)
                receiver.recycle(DISCARD);
        }
    }
    private static final StaticMethodOwner DISCARD = new StaticMethodOwner("Emitters.discard");

    @SuppressWarnings("unused")
    public static <B extends Batch<B>> Orphan<B> collect(Orphan<? extends Emitter<B, ?>> e) {
        var receiver = CollectingReceiver.create(e).takeOwnership(COLLECT);
        try {
            return receiver.take();
        } finally {receiver.recycle(COLLECT);}
    }
    private static final StaticMethodOwner COLLECT = new StaticMethodOwner("Emitters.collect");

    public static <B extends Batch<B>> boolean ask(Orphan<? extends Emitter<B, ?>> e) {
        var receiver = AskReceiver.create(e).takeOwnership(ASK);
        try {
            return receiver.has();
        } finally {receiver.recycle(ASK);}
    }
    private static final StaticMethodOwner ASK = new StaticMethodOwner("Emitters.ask");

    public static void handleEmitError(Receiver<?> downstream,
                                       Emitter<?, ?> upstream,
                                       Throwable emitError,
                                       @Nullable Batch<?> batch) {
        handleEmitError(downstream, upstream, emitError, batch, upstream);
    }

    public static void handleEmitError(Receiver<?> downstream,
                                       Emitter<?, ?> upstream,
                                       Throwable emitError,
                                       @Nullable Batch<?> batch,
                                       @Nullable Object batchOwner) {
        if (ENABLED) journal("deliver failed, rcv=", downstream, ", on", upstream);
        if (upstream.isTerminated()) {
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
        Batch.safeRecycle(batch, batchOwner);
    }

    public static void handleTerminationError(Receiver<?> downstream, Emitter<?, ?> upstream,
                                              Throwable error) {
        if (ENABLED) journal("handleTerminationError, rcv=", downstream, ", on", upstream);
        String name = error.getClass().getSimpleName();
        if (LOG_DEBUG)
            log.info("Ignoring {} while delivering termination to {}", name, downstream, error);
        else
            log.info("Ignoring {} while delivering termination to {}", name, downstream);
    }


}
