package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItCancelledException;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import org.checkerframework.checker.nullness.qual.Nullable;


public class ProcessorBIt<B extends Batch<B>> extends DelegatedControlBIt<B, B> {
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

    protected StringBuilder selfLabel(StreamNodeDOT.Label type) {
        var pt = type == StreamNodeDOT.Label.MINIMAL ? type : StreamNodeDOT.Label.SIMPLE;
        return new StringBuilder().append(processor.label(pt));
    }

    protected void cleanup(@Nullable Throwable error) {
        processor.release();
        if (error != null && !(error instanceof BItCancelledException))
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
            if   (b == null) onTermination(null); //exhausted
            else             onNextBatch(b);
            return b;
        } catch (Throwable t) {
            onTermination(t); //error
            throw t;
        }
    }
}
