package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.ConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchConverter;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.IdBatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.store.batch.IdTranslator.*;
import static com.github.alexishuf.fastersparql.store.index.dict.Dict.NOT_FOUND;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public class StoreBatchType extends IdBatchType<StoreBatch> {
    private static final class StoreBatchFac implements Supplier<StoreBatch> {
        @Override public StoreBatch get() {
            long[] ids = new long[PREFERRED_BATCH_TERMS];
            return new StoreBatch.Concrete(ids, (short)1).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "StoreBatchType.FAC";}
    }
    public static final StoreBatchType STORE = new StoreBatchType();


    private StoreBatchType() {
        super(StoreBatch.class, new StoreBatchFac());
    }

    public static final class Converter implements BatchConverter<StoreBatch> {
        private final int dictId;

        private Converter(int dictId) {this.dictId = dictId;}

        @Override public <I extends Batch<I>> BIt<StoreBatch> convert(BIt<I> in) {
            return STORE.convert(in, dictId);
        }

        @Override public <I extends Batch<I>>
        Orphan<? extends Emitter<StoreBatch, ?> > convert(Orphan<? extends Emitter<I, ?>> in) {
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
            extends ConverterBIt<StoreBatch, S> {
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

    @Override public <I extends Batch<I>>
    Orphan<? extends Emitter<StoreBatch, ?>> convert(Orphan<? extends Emitter<I, ?>> emitter) {
        if (equals(Emitter.peekBatchTypeWild(emitter))) //noinspection unchecked
            return (Orphan<? extends Emitter<StoreBatch, ?>>)emitter;
        throw new UnsupportedOperationException("use convert(Emitter emitter, int dictId)");
    }

    public <I extends Batch<I>> Orphan<? extends Emitter<StoreBatch, ?>>
    convert(Orphan<? extends Emitter<I, ?>> emitter, int dictId) {
        if (Emitter.peekBatchType(emitter) == this) //noinspection unchecked
            return (Orphan<? extends Emitter<StoreBatch, ?>>) emitter;
        return new StoreConverterStage.Concrete<>(emitter, dictId);
    }

    private static sealed class StoreConverterStage<I extends Batch<I>>
            extends ConverterStage<I, StoreBatch, StoreConverterStage<I>> {
        private final int dictId;

        public StoreConverterStage(Orphan<? extends Emitter<I, ?>> upstream,
                                   int dictId) {
            super(STORE, upstream);
            this.dictId = dictId;
        }

        private static final class Concrete<I extends Batch<I>>
                extends StoreConverterStage<I> implements Orphan<StoreConverterStage<I>> {
            public Concrete(Orphan<? extends Emitter<I, ?>> upstream, int dictId) {
                super(upstream, dictId);
            }
            @Override public StoreConverterStage<I> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public void onBatchByCopy(I b) {
            if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(b);
            if (b != null && b.rows > 0) {
                var conv = STORE.createForThread(threadId, cols).takeOwnership(this);
                conv.putConverting(b, dictId);
                downstream.onBatch(conv.releaseOwnership(this));
            }
        }
    }

    @Override public int hashId(long id) {return StoreBatch.hashId(id);}

    @Override public boolean equals(long lId, long rId) {return StoreBatch.equals(lId, rId);}

    @Override public ByteSink<?, ?> appendNT(ByteSink<?, ?> sink, long id, byte[] nullValue) {
        if (id != NOT_FOUND) {
            var lookup = dict(dictId(id)).lookup().takeOwnership(this);
            try {
                var tmp = lookup.get(unsource(id));
                if (tmp != null && tmp.len > 0)
                    return sink.append(tmp);
            } finally {
                lookup.recycle(this);
            }
        }
        return sink.append(nullValue);
    }
}
