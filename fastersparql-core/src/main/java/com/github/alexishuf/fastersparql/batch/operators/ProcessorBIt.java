package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchFilter;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.exceptions.FSCancelledException.isCancel;


public class ProcessorBIt<B extends Batch<B>> extends DelegatedControlBIt<B, B> {
    private static final Logger log = LoggerFactory.getLogger(ProcessorBIt.class);
    protected final BatchProcessor<B> processor;
    protected boolean terminated;

    public ProcessorBIt(BIt<B> delegate, BatchProcessor<B> processor,
                        @Nullable MetricsFeeder metrics) {
        super(delegate, delegate.batchType(), processor.outVars);
        this.processor = processor;
        this.metrics = metrics;
    }

    public static <B extends Batch<B>> BIt<B> project(Vars outVars, BIt<B> in) {
        var p = in.batchType().projector(outVars, in.vars());
        if (p == null)
            return in;
        return new ProcessorBIt<>(in, p, null);
    }

    public static boolean isDedup(BIt<?> it) {
        while (true) {
            if (it instanceof ProcessorBIt<?> p) {
                if (p.processor instanceof BatchFilter<?> f && f.isDedup())
                    return true;
            } else if (it instanceof DelegatedControlBIt<?,?> d) {
                it = d.delegate();
            } else {
                return false;
            }
        }
    }

    protected void cleanup(@Nullable Throwable cause) {
        try {
            if (metrics != null)
                metrics.completeAndDeliver(cause, isCancel(cause));
        } catch (Throwable t) {
            log.error("{}: Failed to deliver metrics {}", this, metrics, t);
        }
        processor.release();
        if (cause != null)
            delegate.close();
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
                if ((b = processor.processInPlace(b)) == null) {
                    delegate.close(); // premature end, likely due to LIMIT clause
                    break;
                } else if (b.rows > 0) {
                    break;
                }
            }
            if      (b       == null) onTermination(null); //exhausted
            else if (metrics != null) metrics.batch(b.rows);
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
