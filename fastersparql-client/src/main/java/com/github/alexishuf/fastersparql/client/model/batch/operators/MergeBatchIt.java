package com.github.alexishuf.fastersparql.client.model.batch.operators;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.BatchIt;
import com.github.alexishuf.fastersparql.client.model.batch.base.BoundedBufferedBatchIt;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MergeBatchIt<T> extends BoundedBufferedBatchIt<T> {
    private final Collection<? extends BatchIt<? extends T>> sources;
    private @MonotonicNonNull ExecutorService workers;
    private boolean stopping;

    public MergeBatchIt(Collection<? extends BatchIt<? extends T>> sources,
                        Class<T> elementClass, String name) {
        super(elementClass, name);
        this.sources = sources;
    }

    /* --- --- helper methods --- --- --- */

    private void ensureStarted() {
        checkOpen();
        if (workers == null) {
            workers = Executors.newVirtualThreadPerTaskExecutor();
            for (BatchIt<? extends T> source : sources)
                workers.submit(() -> drain(source));
        }
    }

    private void drain(BatchIt<? extends T> source) {
        source.minBatch(minBatch).maxBatch(maxBatch())
              .minWait(minWait(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
        try (source) {
            while (!stopping) {
                @SuppressWarnings("unchecked") var batch = (Batch<T>) source.nextBatch();
                if (batch.size > 0) feed(batch);
                else                break;
            }
        }
    }

    /* --- --- overrides --- --- --- */

    @Override protected Batch<T> fetch() {
        ensureStarted();
        return super.fetch();
    }

    @Override protected void cleanup() {
        stopping = true;
        super.cleanup();
        if (workers != null)
            workers.close();
    }
}
