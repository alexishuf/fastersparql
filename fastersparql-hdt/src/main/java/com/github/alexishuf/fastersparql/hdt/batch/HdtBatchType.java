package com.github.alexishuf.fastersparql.hdt.batch;

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
import com.github.alexishuf.fastersparql.model.rope.Rope;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public class HdtBatchType extends IdBatchType<HdtBatch> {
    private static final class HdtBatchFac implements Supplier<HdtBatch> {
        @Override public HdtBatch get() {
            long[] ids = new long[PREFERRED_BATCH_TERMS];
            return new HdtBatch.Concrete(ids, (short)1).takeOwnership(RECYCLED);
        }
        @Override public String toString() {return "HdtStoreBathc.FAC";}
    }

    public static final HdtBatchType HDT = new HdtBatchType();

    public HdtBatchType() {
        super(HdtBatch.class, new HdtBatchFac());
    }

    public static final class Converter implements BatchConverter<HdtBatch> {
        private final int dictId;
        public Converter(int dictId) {this.dictId = dictId;}
        @Override public <I extends Batch<I>> BIt<HdtBatch> convert(BIt<I> in) {
            return HDT.convert(in, dictId);
        }
        @Override public <I extends Batch<I>>
        Orphan<? extends Emitter<HdtBatch, ?>> convert(Orphan<? extends Emitter<I, ?>> in) {
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
            extends ConverterBIt<HdtBatch, S> {
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

    @Override public <I extends Batch<I>> Orphan<? extends Emitter<HdtBatch, ?>>
    convert(Orphan<? extends Emitter<I, ?>> emitter) {
        if (equals(Emitter.peekBatchTypeWild(emitter))) //noinspection unchecked
            return (Orphan<? extends Emitter<HdtBatch, ?>>)emitter;
        throw new UnsupportedOperationException("use convert(Emitter emitter, int dictId)");
    }

    public <I extends Batch<I>> Orphan<? extends Emitter<HdtBatch, ?>>
    convert(Orphan<? extends Emitter<I, ?>> upstream, int dictId) {
        if (Emitter.peekBatchType(upstream) == HDT) //noinspection unchecked
            return (Orphan<? extends Emitter<HdtBatch, ?>>)upstream;
        return new HdtConverterStage.Concrete<>(upstream, dictId);
    }

    private static sealed abstract class HdtConverterStage<I extends Batch<I>>
            extends ConverterStage<I, HdtBatch, HdtConverterStage<I>> {
        private final int dictId;

        public HdtConverterStage(Orphan<? extends Emitter<I, ?>> upstream,
                                 int dictId) {
            super(HDT, upstream);
            this.dictId = dictId;
        }

        private static final class Concrete<I extends Batch<I>>
                extends HdtConverterStage<I> implements Orphan<HdtConverterStage<I>> {
            public Concrete(Orphan<? extends Emitter<I, ?>> upstream, int dictId) {
                super(upstream, dictId);
            }
            @Override public HdtConverterStage<I> takeOwnership(Object o) {return takeOwnership0(o);}
        }


        @Override public void onBatchByCopy(I b) {
            if (EmitterStats.ENABLED && stats != null) stats.onBatchPassThrough(b);
            int rows = b == null ? 0 : b.rows;
            if (rows > 0)  {
                var dst = HDT.createForThread(threadId, cols).takeOwnership(this);
                dst.putConverting(b, dictId);
                downstream.onBatch(dst.releaseOwnership(this));
            }
        }
    }

    @Override public int hashId(long id) {
        if (id == 0) return Rope.FNV_BASIS;
        Term term = IdAccess.toTerm(id);
        return term == null ? Rope.FNV_BASIS : term.hashCode();
    }

    @Override public boolean equals(long lId, long rId) {
        return HdtBatch.equals(lId, rId);
    }

    @Override public ByteSink<?, ?> appendNT(ByteSink<?, ?> sink, long id, byte[] nullValue) {
        CharSequence cs = IdAccess.toString(id);
        if (cs != null && !cs.isEmpty())
            sink.append(cs);
        else
            sink.append(nullValue);
        return sink;
    }
}
