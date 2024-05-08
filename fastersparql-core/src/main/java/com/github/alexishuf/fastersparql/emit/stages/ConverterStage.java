package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.AbstractStage;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class ConverterStage<I extends Batch<I>, O extends  Batch<O>,
                            S extends ConverterStage<I, O, S>>
        extends AbstractStage<I, O, S> {
    @SuppressWarnings("FieldMayBeFinal") private static int nextSurrogateThreadId = 1;
    private static final VarHandle SURR_THREAD_ID;
    static {
        try {
            SURR_THREAD_ID = MethodHandles.lookup().findStaticVarHandle(ConverterStage.class, "nextSurrogateThreadId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final short cols, threadId;
    protected final BatchType<I> upstreamBT;

    protected ConverterStage(BatchType<O> type,
                          Orphan<? extends Emitter<I, ?>> upstream) {
        super(type, Emitter.peekVars(upstream));
        int cols = vars.size(), threadId = (int)SURR_THREAD_ID.getAndAdd(1);
        if (cols > Short.MAX_VALUE)
            throw new IllegalArgumentException("Too many columns");
        this.cols     = (short)cols;
        this.threadId = (short)threadId;
        subscribeTo(upstream);
        this.upstreamBT = this.upstream.batchType();
    }


    public static <I extends Batch<I>, O extends Batch<O>>
    Orphan<? extends ConverterStage<I, O, ?>>
    create(BatchType<O> type, Orphan<? extends Emitter<I, ?>> upstream) {
        return new Concrete<>(type, upstream);
    }

    private static class Concrete<I extends Batch<I>, O extends Batch<O>>
            extends ConverterStage<I, O, Concrete<I, O>>
            implements Orphan<Concrete<I, O>> {
        public Concrete(BatchType<O> type, Orphan<? extends Emitter<I, ?>> upstream) {
            super(type, upstream);
        }
        @Override public Concrete<I, O> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    @Override public String toString() {
        return String.format("%s@%x<-%s",
                batchType.getClass().getSimpleName().replace("BatchType", ""),
                System.identityHashCode(this),
                upstream);
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = new StringBuilder().append(batchType.toString()).append("@");
        sb.append(Integer.toHexString(System.identityHashCode(this)));
        if (type.showStats() && stats != null)
            return stats.appendToLabel(sb).toString();
        return sb.toString();
    }

    @Override public @This S subscribeTo(Orphan<? extends Emitter<I, ?>> emitter) {
        if (!Emitter.peekVars(emitter).equals(vars))
            throw new IllegalArgumentException("Mismatching vars");
        return super.subscribeTo(emitter);
    }

    @Override public final void onBatch(Orphan<I> orphan) {
        I b = orphan.takeOwnership(this);
        onBatchByCopy(b);
        b.recycle(this);
    }
    @Override public void onBatchByCopy(I b) {
        if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(b);
        if (b != null) {
            O dst = batchType.createForThread(threadId, cols).takeOwnership(this);
            dst.putConverting(b);
            downstream.onBatch(dst.releaseOwnership(this));
        }
    }
}
