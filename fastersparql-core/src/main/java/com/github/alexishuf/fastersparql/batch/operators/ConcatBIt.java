package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
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

    public ConcatBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType,
                     Vars vars) {
        super(batchType, vars);
        this.sourcesIt = (this.sources = sources).iterator();
        this.inner = new EmptyBIt<>(batchType, vars);
    }
    
    /* --- --- --- helper --- --- --- */

    @EnsuresNonNullIf(expression = "this.inner", result = true)
    protected boolean nextSource() {
        if (sourcesIt.hasNext()) {
            var source = sourcesIt.next().minBatch(minBatch()).maxBatch(maxBatch())
                                         .minWait(minWait(NANOSECONDS), NANOSECONDS);
            //noinspection DataFlowIssue (inner is @NonNull in ConcatBIt)
            for (B b; (b = this.inner.stealRecycled()) != null; ) {
                if ((b = source.recycle(b)) != null) batchType.recycle(b);
            }
            if (eager) source.tempEager();
            this.inner = source;
            return true;
        }
        return false;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        ExceptionCondenser.closeAll(sourcesIt);
    }

    /* --- --- --- implementations --- --- --- */

    @Override public @Nullable B nextBatch(@Nullable B b) {
        try {
            do {
                //noinspection DataFlowIssue inner != null
                b = inner.nextBatch(b);
                if (b != null)  {
                    eager = false;
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

    @Override public String toString() { return toStringWithOperands(sources); }
}
