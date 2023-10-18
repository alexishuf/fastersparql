package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.operators.IdConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.Batch.asUnpooled;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.util.concurrent.LevelPool.DEF_HUGE_LEVEL_CAPACITY;
import static com.github.alexishuf.fastersparql.util.concurrent.LevelPool.LARGE_MAX_CAPACITY;

public class StoreBatchType extends BatchType<StoreBatch> {
    public static final StoreBatchType INSTANCE = new StoreBatchType();

    static {
        int offers = Runtime.getRuntime().availableProcessors();
        int hugeOffers = Math.min(DEF_HUGE_LEVEL_CAPACITY, offers);
        for (int cap = 1; cap < LARGE_MAX_CAPACITY; cap<<=1) {
            for (int i = 0; i < offers; i++) {
                var b = new StoreBatch(cap, 1);
                b.markPooled();
                INSTANCE.pool.offer(b, cap);
            }
            for (int i = 0; i < hugeOffers; i++) {
                var b = new StoreBatch(cap, 1);
                b.markPooled();
                INSTANCE.pool.offer(b, cap);
            }
        }
    }

    public StoreBatchType() {super(StoreBatch.class);}

    @Override public StoreBatch create(int rows, int cols) {
        return createForTerms(rows > 0 ? rows*cols : cols, cols);
    }

    @Override public StoreBatch createForTerms(int terms, int cols) {
        StoreBatch b = pool.getAtLeast(terms);
        if (b == null)
            return new StoreBatch(terms, cols);
        BatchEvent.Unpooled.record(b);
        return b.clear(cols).markUnpooled();
    }

    @Override
    public StoreBatch empty(@Nullable StoreBatch offer, int rows, int cols) {
        return emptyForTerms(offer, rows > 0 ? rows*cols : cols, cols);
    }
    @Override
    public StoreBatch emptyForTerms(@Nullable StoreBatch offer, int terms, int cols) {
        var b = offer == null ? null : terms > offer.termsCapacity() ? recycle(offer) : offer;
        if (b == null && (b = pool.getAtLeast(terms)) == null)
            return new StoreBatch(terms, cols);
        b.clear(cols);
        if (b != offer)
            BatchEvent.Unpooled.record(b.markUnpooled());
        return b;
    }

    @Override public StoreBatch withCapacity(@Nullable StoreBatch offer, int rows, int cols) {
        if (offer      == null) return createForTerms(rows > 0 ? rows*cols : cols, cols);
        if (offer.cols != cols) throw new IllegalArgumentException("offer.cols != cols");
        int req = Math.max(1, rows+offer.rows)*cols;
        return offer.termsCapacity() >= req ? offer : offer.grown(req, null, null);
    }

    //    @Override
//    public StoreBatch reserved(@Nullable StoreBatch offer, int rows, int cols, int localBytes) {
//        int terms = rows*cols;
//        if (offer != null) {
//            if (offer.hasCapacity(terms, 0)) {
//                offer.clear(cols);
//                return offer;
//            }
//            recycle(offer);
//        }
//        StoreBatch b = pool.getAtLeast(terms);
//        if (b == null)
//            return new StoreBatch(rows, cols);
//        BatchEvent.Unpooled.record(terms);
//        return b.clearAndReserveAndUnpool(rows, cols);
//    }

    @Override public @Nullable StoreBatch recycle(@Nullable StoreBatch b) {
        if (b == null)
            return null;
        BatchEvent.Pooled.record(b.markPooled());
        if (pool.offerToNearest(b, b.termsCapacity()) != null)
            b.markGarbage();
        return null;
    }

    public static final class Converter implements BatchConverter<StoreBatch> {
        private final int dictId;

        private Converter(int dictId) {this.dictId = dictId;}

        @Override public <I extends Batch<I>> BIt<StoreBatch> convert(BIt<I> in) {
            return INSTANCE.convert(in, dictId);
        }

        @Override public <I extends Batch<I>> Emitter<StoreBatch> convert(Emitter<I> in) {
            return INSTANCE.convert(in, dictId);
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
            return out.putConverting(in, dictId, null, null);
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
            super(INSTANCE, upstream);
            this.dictId = dictId;
        }

        @Override public @Nullable I onBatch(I batch) {
            if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(batch);
            int rows = batch == null ? 0 : batch.rows;
            if (rows == 0) return batch;
            StoreBatch dst = INSTANCE.emptyForTerms(asUnpooled(recycled), rows*cols, cols);
            recycled = null;
            dst = dst.putConverting(batch, dictId, null, null);
            recycled = Batch.asPooled(downstream.onBatch(dst));
            return batch;
        }

        @Override public void onRow(I batch, int row) {
            if (EmitterStats.ENABLED && stats != null) stats.onRowPassThrough();
            if (batch == null) return;
            var dst = INSTANCE.emptyForTerms(asUnpooled(recycled), row*cols, cols);
            recycled = null;
            dst = dst.putRowConverting(batch, row, dictId);
            recycled = Batch.asPooled(downstream.onBatch(dst));
        }
    }

    @Override public RowBucket<StoreBatch> createBucket(int rowsCapacity, int cols) {
        return new StoreBatchBucket(rowsCapacity, cols);
    }


    @Override public StoreBatch.@Nullable Merger projector(Vars out, Vars in) {
        int[] sources = projectorSources(out, in);
        return sources == null ? null : new StoreBatch.Merger(this, out, sources);
    }

    @Override public StoreBatch.@NonNull Merger merger(Vars out, Vars left, Vars right) {
        return new StoreBatch.Merger(this, out, mergerSources(out, left, right));
    }

    @Override public StoreBatch.Filter filter(Vars out, Vars in, RowFilter<StoreBatch> filter,
                                              BatchFilter<StoreBatch> before) {
        return new StoreBatch.Filter(this, out, projector(out, in), filter, before);
    }

    @Override public StoreBatch.Filter filter(Vars vars, RowFilter<StoreBatch> filter,
                                              BatchFilter<StoreBatch> before) {
        return new StoreBatch.Filter(this, vars, null, filter, before);
    }

}
