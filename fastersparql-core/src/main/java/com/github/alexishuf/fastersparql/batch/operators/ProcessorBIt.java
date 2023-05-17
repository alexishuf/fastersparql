package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;

public class ProcessorBIt<B extends Batch<B>> extends DelegatedControlBIt<B, B> {
    private static final Logger log = LoggerFactory.getLogger(ProcessorBIt.class);
    protected final BatchProcessor<B> processor;
    protected final @Nullable Metrics metrics;
    protected boolean terminated;

    public ProcessorBIt(BIt<B> delegate, Vars vars, BatchProcessor<B> processor,
                        @Nullable Metrics metrics) {
        super(delegate, delegate.batchType(), vars);
        this.processor = processor;
        this.metrics = metrics;
    }

    protected void cleanup(@Nullable Throwable cause) {
        processor.release();
        if (cause != null)
            delegate.close();
        if (metrics != null)
            metrics.complete(cause, isClosedFor(cause, delegate)).deliver();
    }

    protected final void onTermination(@Nullable Throwable cause) {
        if (terminated) return;
        terminated = true;
        try {
            cleanup(cause);
        } catch (Throwable t) {
            log.error("Ignoring failed cleanup() for {}", this, t);
        }
    }

    @Override public @Nullable B nextBatch(B b) {
        try {
            while ((b = delegate.nextBatch(b)) != null) {
                if ((b = processor.processInPlace(b)) != null && b.rows > 0)
                    break;
            }
            if (b == null) onTermination(null); //exhausted
            else if (metrics != null) metrics.rowsEmitted(b.rows);
            return b;
        } catch (Throwable t) {
            onTermination(t); //error
            throw t;
        }
    }

    @Override public @Nullable B recycle(@Nullable B batch) {
        return processor.recycle(batch);
    }

    @Override public @Nullable B stealRecycled() {
        return processor.stealRecycled();
    }
}
