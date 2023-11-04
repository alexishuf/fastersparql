package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.IdConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import org.checkerframework.checker.nullness.qual.Nullable;

public class StoreBatchType extends IdBatchType<StoreBatch> {
    public static final StoreBatchType STORE = new StoreBatchType();

    private static final class StoreBatchFactory implements BatchPool.Factory<StoreBatch> {
        @Override public StoreBatch create() {
            var b = new StoreBatch(PREFERRED_BATCH_TERMS, (short)1, false);
            b.markPooled();
            return b;
        }
    }

    private static final class SizedStoreBatchFactory implements LevelBatchPool.Factory<StoreBatch> {
        @Override public StoreBatch create(int terms) {
            StoreBatch b = new StoreBatch(terms, (short) 1, true);
            b.markPooled();
            return b;
        }
    }

    private StoreBatchType() {
        super(StoreBatch.class, new StoreBatchFactory(), new SizedStoreBatchFactory());
    }

    public static final class Converter implements BatchConverter<StoreBatch> {
        private final int dictId;

        private Converter(int dictId) {this.dictId = dictId;}

        @Override public <I extends Batch<I>> BIt<StoreBatch> convert(BIt<I> in) {
            return STORE.convert(in, dictId);
        }

        @Override public <I extends Batch<I>> Emitter<StoreBatch> convert(Emitter<I> in) {
            return STORE.convert(in, dictId);
        }

    }

    public Converter converter(int dictId) { return new Converter(dictId); }

    public <O extends Batch<O>> BIt<StoreBatch> convert(BIt<O> other, int dictId) {
        if (equals(other.batchType())) //noinspection unchecked
            return (BIt<StoreBatch>) other;
        return new StoreBatchConverterBIt<>(other, this, dictId);
    }

    private static class StoreBatchConverterBIt<S extends Batch<S>>
            extends IdConverterBIt<StoreBatch, S> {
        private final int dictId;

        public StoreBatchConverterBIt(BIt<S> delegate, BatchType<StoreBatch> batchType,
                                    int dictId) {
            super(delegate, batchType);
            this.dictId = dictId;
        }

        @Override protected StoreBatch putConverting(StoreBatch out, S in) {
            return out.putConverting(in, dictId);
        }
    }

    @Override public <I extends Batch<I>> Emitter<StoreBatch> convert(Emitter<I> emitter) {
        if (equals(emitter.batchType())) //noinspection unchecked
            return (Emitter<StoreBatch>) emitter;
        throw new UnsupportedOperationException("use convert(Emitter emitter, int dictId)");
    }

    public <I extends Batch<I>> Emitter<StoreBatch>
    convert(Emitter<I> emitter, int dictId) {
        if (emitter.batchType() == this) //noinspection unchecked
            return (Emitter<StoreBatch>) emitter;
        return new StoreConverterStage<>(emitter, dictId);
    }

    private static final class StoreConverterStage<I extends Batch<I>>
            extends ConverterStage<I, StoreBatch> {
        private final int dictId;

        public StoreConverterStage(Emitter<I> upstream, int dictId) {
            super(STORE, upstream);
            this.dictId = dictId;
        }

        @Override public @Nullable I onBatch(I b) {
            if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(b);
            if (b != null && b.rows > 0) {
                var conv = STORE.createForThread(threadId, cols).putConverting(b, dictId);
                batchType.recycleForThread(threadId, downstream.onBatch(conv));
            }
            return b;
        }

        @Override public void onRow(I batch, int row) {
            if (EmitterStats.ENABLED && stats != null) stats.onRowPassThrough();
            var conv = STORE.createForThread(threadId, cols);
            conv.putRowConverting(batch, row, dictId);
            batchType.recycleForThread(threadId, downstream.onBatch(conv));
        }
    }
}
