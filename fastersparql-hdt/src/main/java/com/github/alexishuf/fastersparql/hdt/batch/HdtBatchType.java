package com.github.alexishuf.fastersparql.hdt.batch;

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

public class HdtBatchType extends BatchType<HdtBatch> {
    public static final HdtBatchType INSTANCE = new HdtBatchType();

    public HdtBatchType() {super(HdtBatch.class);}

    @Override public HdtBatch create(int rows, int cols) {
        return createForTerms(rows > 0 ? rows*cols : cols, cols);
    }

    @Override public HdtBatch createForTerms(int terms, int cols) {
        HdtBatch b = pool.getAtLeast(terms);
        if (b == null)
            return new HdtBatch(terms, cols);
        BatchEvent.Unpooled.record(b);
        return b.clear(cols).markUnpooled();
    }

    @Override public HdtBatch empty(@Nullable HdtBatch offer, int rows, int cols) {
        return emptyForTerms(offer, rows > 0 ? rows*cols : cols, cols);
    }
    @Override public HdtBatch emptyForTerms(@Nullable HdtBatch offer, int terms, int cols) {
        var b = offer == null ? null : terms > offer.termsCapacity() ? recycle(offer) : offer;
        if (b == null && (b = pool.getAtLeast(terms)) == null)
            return new HdtBatch(terms, cols);
        b.clear(cols);
        if (b != offer)
            BatchEvent.Unpooled.record(b.markUnpooled());
        return b;
    }

    @Override public HdtBatch withCapacity(@Nullable HdtBatch offer, int rows, int cols) {
        if (offer      == null) return createForTerms(rows > 0 ? rows*cols : cols, cols);
        if (offer.cols != cols) throw new IllegalArgumentException("offer.cols != cols");
        int req = Math.max(1, rows+offer.rows)*cols;
        return offer.termsCapacity() >= req ? offer : offer.grown(req, null, null);
    }

//    @Override
//    public HdtBatch reserved(@Nullable HdtBatch offer, int rows, int cols, int localBytes) {
//        int terms = rows*cols;
//        if (offer != null) {
//            if (offer.hasCapacity(terms, 0)) {
//                offer.clear(cols);
//                return offer;
//            } else {
//                recycle(offer);
//            }
//        }
//        var b = pool.getAtLeast(terms);
//        if (b == null)
//            return new HdtBatch(rows, cols);
//        BatchEvent.Unpooled.record(terms);
//        return b.clearAndReserveAndUnpool(rows, cols);
//    }

    @Override public @Nullable HdtBatch recycle(@Nullable HdtBatch b) {
        if (b == null)
            return null;
        BatchEvent.Pooled.record(b.markPooled());
        if (pool.offerToNearest(b, b.termsCapacity()) != null)
            b.markGarbage();
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

        @Override public @Nullable I onBatch(I batch) {
            if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(batch);
            int rows = batch == null ? 0 : batch.rows;
            if (rows == 0) return batch;
            var dst = INSTANCE.emptyForTerms(asUnpooled(recycled), rows*cols, cols);
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

    @Override public RowBucket<HdtBatch> createBucket(int rows, int cols) {
        return new HdtBatchBucket(rows, cols);
    }

    @Override public HdtBatch.@Nullable Merger projector(Vars out, Vars in) {
        int[] sources = projectorSources(out, in);
        return sources == null ? null : new HdtBatch.Merger(this, out, sources);
    }

    @Override public HdtBatch.@NonNull Merger merger(Vars out, Vars left, Vars right) {
        return new HdtBatch.Merger(this, out, mergerSources(out, left, right));
    }

    @Override public HdtBatch.Filter filter(Vars out, Vars in, RowFilter<HdtBatch> filter,
                                            BatchFilter<HdtBatch> before) {
        return new HdtBatch.Filter(this, out, projector(out, in), filter, before);
    }

    @Override public HdtBatch.Filter filter(Vars vars, RowFilter<HdtBatch> filter,
                                            BatchFilter<HdtBatch> before) {
        return new HdtBatch.Filter(this, vars, null, filter, before);
    }

}
