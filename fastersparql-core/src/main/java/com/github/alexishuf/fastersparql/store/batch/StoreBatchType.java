package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.operators.IdConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Merger;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.stages.ConverterStage;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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

    @Override public StoreBatch create(int rowsCapacity, int cols, int localBytes) {
        int capacity = rowsCapacity*cols;
        StoreBatch b = pool.getAtLeast(capacity);
        if (b == null)
            return new StoreBatch(rowsCapacity, cols);
        BatchEvent.Unpooled.record(12*capacity);
        b.clearAndUnpool(cols);
        return b;
    }

    @Override public @Nullable StoreBatch poll(int rowsCapacity, int cols, int localBytes) {
        int terms = rowsCapacity * cols;
        var b = pool.getAtLeast(terms);
        if (b != null) {
            if (b.hasCapacity(terms, 0)) {
                BatchEvent.Unpooled.record(terms);
                return b.clearAndUnpool(cols);
            } else {
                if (pool.shared.offerToNearest(b, terms) != null)
                    b.markGarbage();
            }
        }
        return null;
    }

    @Override
    public StoreBatch empty(@Nullable StoreBatch offer, int rows, int cols, int localBytes) {
        if (offer != null) {
            offer.clear(cols);
            return offer;
        }
        return create(rows, cols, localBytes);
    }

    @Override
    public StoreBatch reserved(@Nullable StoreBatch offer, int rows, int cols, int localBytes) {
        int terms = rows*cols;
        if (offer != null) {
            if (offer.hasCapacity(terms, 0)) {
                offer.clear(cols);
                return offer;
            }
            recycle(offer);
        }
        StoreBatch b = pool.getAtLeast(terms);
        if (b == null)
            return new StoreBatch(rows, cols);
        BatchEvent.Unpooled.record(terms);
        return b.clearAndReserveAndUnpool(rows, cols);
    }

    @Override public @Nullable StoreBatch recycle(@Nullable StoreBatch batch) {
        if (batch == null) return null;
        int capacity = batch.directBytesCapacity();
        batch.markPooled();
        if (pool.offerToNearest(batch, capacity) == null) {
            BatchEvent.Pooled.record(12*capacity);
        } else {
            batch.recycleInternals(); // could not pool batch, try recycling arr and hashes
            BatchEvent.Garbage.record(12*capacity);
        }
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

        @Override protected StoreBatch putConverting(StoreBatch dest, I input) {
            return dest.putConverting(input, dictId, null, null);
        }

        @Override protected void putRowConverting(StoreBatch dest, I input, int row) {
            dest.putRowConverting(input, row, dictId);
        }
    }

    @Override public RowBucket<StoreBatch> createBucket(int rowsCapacity, int cols) {
        return new StoreBatchBucket(rowsCapacity, cols);
    }


    @Override public @Nullable Merger<StoreBatch> projector(Vars out, Vars in) {
        int[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger<>(this, out, sources);
    }

    @Override public @NonNull Merger<StoreBatch> merger(Vars out, Vars left, Vars right) {
        return new Merger<>(this, out, mergerSources(out, left, right));
    }

    @Override public Filter<StoreBatch> filter(Vars out, Vars in, RowFilter<StoreBatch> filter,
                                               BatchFilter<StoreBatch> before) {
        return new Filter<>(this, out, projector(out, in), filter, before);
    }

    @Override public Filter<StoreBatch> filter(Vars vars, RowFilter<StoreBatch> filter,
                                               BatchFilter<StoreBatch> before) {
        return new Filter<>(this, vars, null, filter, before);
    }

}
