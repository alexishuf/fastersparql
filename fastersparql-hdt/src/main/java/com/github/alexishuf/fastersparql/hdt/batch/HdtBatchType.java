package com.github.alexishuf.fastersparql.hdt.batch;

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

public class HdtBatchType extends BatchType<HdtBatch> {
    public static final HdtBatchType INSTANCE = new HdtBatchType();

    public HdtBatchType() {super(HdtBatch.class);}

    @Override public HdtBatch create(int rowsCapacity, int cols, int localBytes) {
        int capacity = rowsCapacity * cols;
        HdtBatch b = pool.getAtLeast(capacity);
        if (b == null)
            return new HdtBatch(rowsCapacity, cols);
        BatchEvent.Unpooled.record(12*capacity);
        return b.clearAndUnpool(cols);
    }

    @Override public @Nullable HdtBatch poll(int rowsCapacity, int cols, int localBytes) {
        int terms = rowsCapacity*cols;
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

    @Override public HdtBatch empty(@Nullable HdtBatch offer, int rows, int cols, int localBytes) {
        if (offer != null) {
            offer.clear(cols);
            return offer;
        }
        return create(rows, cols, 0);
    }

    @Override
    public HdtBatch reserved(@Nullable HdtBatch offer, int rows, int cols, int localBytes) {
        int terms = rows*cols;
        if (offer != null) {
            if (offer.hasCapacity(terms, 0)) {
                offer.clear(cols);
                return offer;
            } else {
                recycle(offer);
            }
        }
        var b = pool.getAtLeast(terms);
        if (b == null)
            return new HdtBatch(rows, cols);
        BatchEvent.Unpooled.record(terms);
        return b.clearAndReserveAndUnpool(rows, cols);
    }

    @Override public @Nullable HdtBatch recycle(@Nullable HdtBatch batch) {
        if (batch == null) return null;
        batch.markPooled();
        int capacity = batch.directBytesCapacity();
        if (pool.offerToNearest(batch, capacity) == null) {
            BatchEvent.Pooled.record(12*capacity);
        } else {
            batch.recycleInternals();
            BatchEvent.Garbage.record(12*capacity);
        }
        return null;
    }

    public static final class Converter implements BatchConverter<HdtBatch> {
        private final int dictId;
        public Converter(int dictId) {this.dictId = dictId;}
        @Override public <I extends Batch<I>> BIt<HdtBatch> convert(BIt<I> in) {
            return INSTANCE.convert(in, dictId);
        }
        @Override public <I extends Batch<I>> Emitter<HdtBatch> convert(Emitter<I> in) {
            return INSTANCE.convert(in, dictId);
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
            return out.putConverting(in, dictId, null, null);
        }
    }

    @Override public <I extends Batch<I>> Emitter<HdtBatch> convert(Emitter<I> emitter) {
        if (equals(emitter.batchType())) //noinspection unchecked
            return (Emitter<HdtBatch>) emitter;
        throw new UnsupportedOperationException("use convert(Emitter emitter, int dictId)");
    }

    public <I extends Batch<I>> Emitter<HdtBatch>
    convert(Emitter<I> upstream, int dictId) {
        if (upstream.batchType() == INSTANCE) //noinspection unchecked
            return (Emitter<HdtBatch>) upstream;
        return new HdtConverterStage<>(upstream, dictId);
    }

    private static final class HdtConverterStage<I extends Batch<I>>
            extends ConverterStage<I, HdtBatch> {
        private final int dictId;

        public HdtConverterStage(Emitter<I> upstream, int dictId) {
            super(INSTANCE, upstream);
            this.dictId = dictId;
        }

        @Override protected HdtBatch putConverting(HdtBatch dest, I input) {
            return dest.putConverting(input, dictId, null, null);
        }

        @Override protected void putRowConverting(HdtBatch dest, I input, int row) {
            dest.putRowConverting(input, row, dictId);
        }
    }

    @Override public RowBucket<HdtBatch> createBucket(int rowsCapacity, int cols) {
        return new HdtBatchBucket(rowsCapacity, cols);
    }

    @Override public @Nullable Merger<HdtBatch> projector(Vars out, Vars in) {
        int[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger<>(this, out, sources);
    }

    @Override public @NonNull Merger<HdtBatch> merger(Vars out, Vars left, Vars right) {
        return new Merger<>(this, out, mergerSources(out, left, right));
    }

    @Override public Filter<HdtBatch> filter(Vars out, Vars in, RowFilter<HdtBatch> filter,
                                             BatchFilter<HdtBatch> before) {
        return new Filter<>(this, out, projector(out, in), filter, before);
    }

    @Override public Filter<HdtBatch> filter(Vars vars, RowFilter<HdtBatch> filter,
                                             BatchFilter<HdtBatch> before) {
        return new Filter<>(this, vars, null, filter, before);
    }

}
