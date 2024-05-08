package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

public abstract class AskReceiver<B extends Batch<B>>
        extends AbstractOwned<AskReceiver<B>>
        implements Receiver<B> {
    private static final Logger log = LoggerFactory.getLogger(AskReceiver.class);
    private static final boolean DEBUG_ENABLED = log.isDebugEnabled();
    private static final VarHandle RESULTS;
    static {
        try {
            RESULTS = MethodHandles.lookup().findVarHandle(AskReceiver.class, "plainResult", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Emitter<B, ?> up;
    @SuppressWarnings("FieldMayBeFinal") private int plainResult = -1;
    private Thread consumer;
    private int state;
    private final EmitterStats stats = EmitterStats.createIfEnabled();

    public static <B extends Batch<B>> Orphan<AskReceiver<B>>
    create(Orphan<? extends Emitter<B, ?>> upstreamOrphan) {return new Concrete<>(upstreamOrphan);}

    protected AskReceiver(Orphan<? extends Emitter<B, ?>> upstreamOrphan) {
        up = upstreamOrphan.takeOwnership(this);
        up.subscribe(this);
    }

    @Override public @Nullable AskReceiver<B> recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        up.cancel();
        return null;
    }

    private static final class Concrete<B extends Batch<B>> extends AskReceiver<B>
            implements Orphan<AskReceiver<B>> {
        public Concrete(Orphan<? extends Emitter<B, ?>> upstreamOrphan) {super(upstreamOrphan);}
        @Override public AskReceiver<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    public boolean request() {
        if (state == Stateful.CREATED)
            return false;
        state    = Stateful.ACTIVE;
        consumer = Thread.currentThread();
        up.request(1);
        return true;
    }

    public boolean has() {
        if (!request())
            consumer = Thread.currentThread();
        int results;
        while ((results=(int)RESULTS.getAcquire(this)) == -1)
            LockSupport.park(this);
        state |= Stateful.IS_TERM_DELIVERED;
        return results != 0;
    }

    /* --- --- --- StreamNode --- --- --- */

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Stream.of(up);
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = new StringBuilder().append("Ask");
        if (type.showState()) {
            sb.append('[').append(Stateful.Flags.DEFAULT.render(state));
            if (plainResult != 0) sb.append(",HAS");
            sb.append(']');
        }
        sb.append('(').append(up.label(StreamNodeDOT.Label.MINIMAL)).append(')');
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    @Override public String toString() {
        return label(StreamNodeDOT.Label.SIMPLE);
    }

    /* --- --- --- Receiver --- --- --- */

    @Override public void onBatch(Orphan<B> orphan) {
        B b = orphan.takeOwnership(this);
        onBatchByCopy(b);
        b.recycle(this);
    }

    @Override public void onBatchByCopy(B batch) {
        if (batch == null)
            return;
        if (batch.rows > 0) {
            if ((int)RESULTS.getAndSetRelease(this, 1) > 0)
                up.cancel(); // upstream should be a LIMIT 1 filter, if it was not, stop emitting
            Unparker.unpark(consumer);
        }
    }

    private void onTermination(int state) {
        this.state = state;
        if (plainResult == -1)
            RESULTS.setRelease(this, 0);
        Unparker.unpark(consumer);
    }

    @Override public void onComplete() { onTermination(Stateful.COMPLETED); }
    @Override public void onCancelled() { onTermination(Stateful.CANCELLED); }

    @Override public void onError(Throwable cause) {
        if (DEBUG_ENABLED)
            log.debug("Ignoring error on AskReceiver for {}", up, cause);
        else
            log.info("Ignoring {} on AskReceiver for {}", cause.getClass().getSimpleName(), up);
        onTermination(Stateful.FAILED);
    }
}
