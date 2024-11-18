package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadFailedException;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.owned.Guard;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ConcatBIt<B extends Batch<B>> extends AbstractFlatMapBIt<B> {
    private static final Logger log = LoggerFactory.getLogger(ConcatBIt.class);
    private final Collection<? extends BIt<B>> sources;
    private final Iterator<? extends BIt<B>> sourcesIt;
    protected int sourceIdx = -1;
    protected @Nullable BatchProcessor<B, ?> processor;

    public ConcatBIt(Collection<? extends BIt<B>> sources, BatchType<B> batchType, Vars vars) {
        super(vars, EmptyBIt.of(batchType));
        this.sourcesIt = (this.sources = sources).iterator();
    }

    /* --- --- --- helper --- --- --- */

    protected boolean nextSource() {
        try {
            inner.close();
        } catch (Throwable t) {
            log.error("inner.close() failed on nextSource() this={}, inner={}", this, inner, t);
        }
        if (processor != null)
            processor = Owned.safeRecycle(processor, this);
        if (sourcesIt.hasNext()) {
            var source = sourcesIt.next().minBatch(minBatch()).maxBatch(maxBatch())
                                         .minWait(minWait(NANOSECONDS), NANOSECONDS);
            if (eager)
                source.tempEager();
            inner = source;
            ++sourceIdx;
            var orphan = createProcessor(source, sourceIdx);
            processor = orphan == null ? null : orphan.takeOwnership(this);
            return true;
        }
        return false;
    }

    protected @Nullable Orphan<? extends BatchProcessor<B, ?>>
    createProcessor(BIt<B> source, int sourceIdx) {
        return batchType.projector(vars, source.vars());
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (processor != null) {
            var p = processor;
            processor = null;
            Owned.safeRecycle(p, this);
        }
        ExceptionCondenser.closeAll(sourcesIt);
    }

    /* --- --- --- implementations --- --- --- */

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return sources.stream();
    }

    @Override public @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer) {
        try (var g = new Guard.BatchGuard<B>(this)) {
            g.set(offer);
            do {
                var p = processor;
                while (g.nextBatch(inner) != null) {
                    lock();
                    try {
                        if (isTerminated()) {
                            break;
                        } else if (p != null) {
                            B b = g.set(p.processInPlace(g.poll()));
                            if      (b      == null) break;
                            else if (b.rows ==    0) continue;
                        }
                    } finally { unlock(); }
                    return onNextBatch(g.take());
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
