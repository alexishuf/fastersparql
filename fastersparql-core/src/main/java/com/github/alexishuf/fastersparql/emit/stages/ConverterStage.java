package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.AbstractStage;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public class ConverterStage<I extends Batch<I>, O extends  Batch<O>> extends AbstractStage<I, O> {
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

    public ConverterStage(BatchType<O> type, Emitter<I> upstream) {
        super(type, upstream.vars());
        int cols = vars.size(), threadId = (int)SURR_THREAD_ID.getAndAdd(1);
        if (cols > Short.MAX_VALUE)
            throw new IllegalArgumentException("Too amny columns");
        this.cols     = (short)cols;
        this.threadId = (short)threadId;
        subscribeTo(upstream);
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

    @Override public @This ConverterStage<I, O> subscribeTo(Emitter<I> emitter) {
        if (!emitter.vars().equals(vars))
            throw new IllegalArgumentException("Mismatching vars");
        super.subscribeTo(emitter);
        return this;
    }


    @Override public @Nullable I onBatch(I b) {
        if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(b);
        if (b != null) {
            O dst = batchType.createForThread(threadId, cols);
            dst.putConverting(b);
            batchType.recycleForThread(threadId, downstream.onBatch(dst));
        }
        return b;
    }
}
