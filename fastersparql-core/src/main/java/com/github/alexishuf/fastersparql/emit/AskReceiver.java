package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

public class AskReceiver<B extends Batch<B>> implements Receiver<B> {
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

    private final Emitter<B> up;
    @SuppressWarnings("FieldMayBeFinal") private int plainResult = -1;
    private Thread consumer;
    private int state;
    private final EmitterStats stats = EmitterStats.createIfEnabled();

    public AskReceiver(Emitter<B> up) {
        this.up = up;
        up.subscribe(this);
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

    @Override public @Nullable B onBatch(B batch) {
        if (batch.rows > 0) {
            RESULTS.setRelease(this, 1);
            Unparker.unpark(consumer);
        }
        return batch;
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
