package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Iterator;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ConcatBIt<B extends Batch<B>> extends AbstractFlatMapBIt<B> {
    private final Collection<? extends BIt<B>> sources;
    private final Iterator<? extends BIt<B>> sourcesIt;
    protected int sourceIdx = -1;
    protected BatchProcessor<B> processor;

    public ConcatBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType, Vars vars) {
        super(vars, EmptyBIt.of(batchType));
        this.sourcesIt = (this.sources = sources).iterator();
    }

    /* --- --- --- helper --- --- --- */

    @EnsuresNonNullIf(expression = "this.inner", result = true)
    protected boolean nextSource() {
        if (sourcesIt.hasNext()) {
            var source = sourcesIt.next().minBatch(minBatch()).maxBatch(maxBatch())
                                         .minWait(minWait(NANOSECONDS), NANOSECONDS);
            for (B b; (b = this.inner.stealRecycled()) != null; ) {
                if ((b = source.recycle(b)) != null) batchType.recycle(b);
            }
            if (eager) source.tempEager();
            inner = source;
            ++sourceIdx;
            processor = createProcessor(source, sourceIdx);
            return true;
        }
        return false;
    }

    protected @Nullable BatchProcessor<B> createProcessor(BIt<B> source, int sourceIdx) {
        return batchType.projector(vars, source.vars());
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (processor != null) {
            processor.release();
            processor = null;
        }
        ExceptionCondenser.closeAll(sourcesIt);
    }

    /* --- --- --- implementations --- --- --- */

    @Override public @Nullable B nextBatch(@Nullable B b) {
        try {
            do {
                BatchProcessor<B> p = processor;
                while ((b = inner.nextBatch(b)) != null) {
                    if (p != null && ((b = p.processInPlace(b)) == null || b.rows == 0)) continue;
                    onBatch(b);
                    return b;
                }
            } while (nextSource());
            onTermination(null);
            return null;
        } catch (Throwable t) {
            onTermination(t);
            throw new BItReadFailedException(this, t);
        }
    }

    @Override public @Nullable B recycle(B batch) {
        if (batch != null && super.recycle(batch) != null && processor != null)
            return processor.recycle(batch);
        return null;
    }

    @Override public String toString() { return toStringWithOperands(sources); }
}
