package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.exceptions.FSCancelledException.isCancel;


public class ProcessorBIt<B extends Batch<B>> extends DelegatedControlBIt<B, B> {
    private static final Logger log = LoggerFactory.getLogger(ProcessorBIt.class);
    protected final BatchProcessor<B> processor;

    public ProcessorBIt(BIt<B> delegate, BatchProcessor<B> processor,
                        @Nullable MetricsFeeder metrics) {
        super(delegate, delegate.batchType(), processor.vars);
        this.processor = processor;
        this.metrics = metrics;
    }

    public static <B extends Batch<B>> BIt<B> project(Vars outVars, BIt<B> in) {
        var p = in.batchType().projector(outVars, in.vars());
        if (p == null)
            return in;
        return new ProcessorBIt<>(in, p, null);
    }

    @Override public String label(StreamNodeDOT.Label type) {
        return processor.label(type);
    }

    protected void cleanup(boolean cancelled, @Nullable Throwable error) {
        try {
            if (metrics != null)
                metrics.completeAndDeliver(error, isCancel(error));
        } catch (Throwable t) {
            log.error("{}: Failed to deliver metrics {}", this, metrics, t);
        }
        processor.release();
        if (error != null)
            delegate.close();
    }

    @Override public @Nullable B nextBatch(B b) {
        try {
            while ((b = delegate.nextBatch(b)) != null) {
                lock();
                try {
                    if (isTerminated()) {
                        b = batchType.recycle(b);
                        break;
                    } else if ((b = processor.processInPlace(b)) == null) {
                        delegate.close(); // premature end, likely due to LIMIT clause
                        break;
                    } else if (b.rows > 0) {
                        break;
                    }
                } finally { unlock(); }
            }
            if      (b       == null) onTermination(false, null); //exhausted
            else if (metrics != null) metrics.batch(b.totalRows());
            return b;
        } catch (Throwable t) {
            onTermination(false, t); //error
            throw t;
        }
    }
}
