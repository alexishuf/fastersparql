package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.base.DelegatedControlBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ProcessorBIt<B extends Batch<B>> extends DelegatedControlBIt<B, B> {
    protected final BatchProcessor<B> processor;
    private boolean terminated;

    public ProcessorBIt(BIt<B> delegate, Vars vars, BatchProcessor<B> processor) {
        super(delegate, delegate.batchType(), vars);
        this.processor = processor;
    }

    protected void cleanup(@Nullable Throwable cause) {
        processor.release();
        if (cause != null)
            delegate.close();
    }

    protected final void onTermination(@Nullable Throwable cause) {
        if (terminated) return;
        terminated = true;
        cleanup(cause);
    }

    @Override public @Nullable B nextBatch(B b) {
        try {
            while ((b = delegate.nextBatch(b)) != null) {
                if ((b = processor.processInPlace(b)) != null && b.rows > 0) return b;
            }
            onTermination(null); //exhausted
            return null;
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
