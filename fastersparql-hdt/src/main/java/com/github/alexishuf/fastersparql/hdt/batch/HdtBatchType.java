package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.operators.IdConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import org.checkerframework.checker.nullness.qual.Nullable;

public class HdtBatchType extends IdBatchType<HdtBatch> {
    public static final HdtBatchType HDT = new HdtBatchType();

    static {
        WeakDedup.registerBatchType(HDT);
    }

    private static final class HdtBatchFactory implements BatchPool.Factory<HdtBatch> {
        @Override public HdtBatch create() {
            var b = new HdtBatch(PREFERRED_BATCH_TERMS, (short)1, false);
            b.markPooled();
            return b;
        }
    }

    private static final class SizedHdtBatchFactory implements LevelBatchPool.Factory<HdtBatch> {
        @Override public HdtBatch create(int terms) {
            HdtBatch b = new HdtBatch(terms, (short) 1, true);
            b.markPooled();
            return b;
        }
    }

    public HdtBatchType() {
        super(HdtBatch.class, new HdtBatchFactory(), new SizedHdtBatchFactory());
    }

    public static final class Converter implements BatchConverter<HdtBatch> {
        private final int dictId;
        public Converter(int dictId) {this.dictId = dictId;}
        @Override public <I extends Batch<I>> BIt<HdtBatch> convert(BIt<I> in) {
            return HDT.convert(in, dictId);
        }
        @Override public <I extends Batch<I>> Emitter<HdtBatch> convert(Emitter<I> in) {
            return HDT.convert(in, dictId);
        }
    }

    public Converter converter(int dictId) { return new Converter(dictId); }

    public <O extends Batch<O>> BIt<HdtBatch> convert(BIt<O> other, int dictId) {
        if (equals(other.batchType())) //noinspection unchecked
            return (BIt<HdtBatch>) other;
        return new HdtBatchConverterBIt<>(other, this, dictId);
    }

    private static class HdtBatchConverterBIt<S extends Batch<S>>
            extends IdConverterBIt<HdtBatch, S> {
        private final int dictId;

        public HdtBatchConverterBIt(BIt<S> delegate, BatchType<HdtBatch> batchType,
                                    int dictId) {
            super(delegate, batchType);
            this.dictId = dictId;
        }

        @Override protected HdtBatch putConverting(HdtBatch out, S in) {
            return out.putConverting(in, dictId);
        }
    }

    @Override public <I extends Batch<I>> Emitter<HdtBatch> convert(Emitter<I> emitter) {
        if (equals(emitter.batchType())) //noinspection unchecked
            return (Emitter<HdtBatch>) emitter;
        throw new UnsupportedOperationException("use convert(Emitter emitter, int dictId)");
    }

    public <I extends Batch<I>> Emitter<HdtBatch>
    convert(Emitter<I> upstream, int dictId) {
        if (upstream.batchType() == HDT) //noinspection unchecked
            return (Emitter<HdtBatch>) upstream;
        return new HdtConverterStage<>(upstream, dictId);
    }

    private static final class HdtConverterStage<I extends Batch<I>>
            extends ConverterStage<I, HdtBatch> {
        private final int dictId;

        public HdtConverterStage(Emitter<I> upstream, int dictId) {
            super(HDT, upstream);
            this.dictId = dictId;
        }

        @Override public @Nullable I onBatch(I b) {
            if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(b);
            int rows = b == null ? 0 : b.rows;
            if (rows == 0) return b;
            var dst = HDT.createForThread(threadId, cols).putConverting(b, dictId);
            batchType.recycleForThread(threadId, downstream.onBatch(dst));
            return b;
        }
    }
}
