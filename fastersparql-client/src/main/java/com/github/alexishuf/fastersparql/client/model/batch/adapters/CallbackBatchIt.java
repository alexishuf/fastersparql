package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.base.BoundedBufferedBatchIt;
import com.github.alexishuf.fastersparql.client.util.async.RuntimeExecutionException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class CallbackBatchIt<T> extends BoundedBufferedBatchIt<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);

    /* --- --- --- constructors --- --- --- */

    public CallbackBatchIt(Class<T> elementClass) {
        this(elementClass, null);
    }

    public CallbackBatchIt(Class<T> elementClass, @Nullable String name) {
        super(elementClass, name == null ? "CallbackBatchIterator-"+nextId.getAndIncrement() : name);
    }


    /* --- --- --- methods --- --- --- */
    
    /** Queue an {@code item} for delivery in a future batch. */
    @Override public void feed(T item) { super.feed(item); }

    /** Feed an entire batch. This may be more efficient than feeding each element */
    @Override protected void feed(Batch<T> batch) { super.feed(batch); }

    /**
     * Mark the iterator as complete after all already queued batches are consumed.
     *
     * @param error the error that caused the completion. If non-null it will be wrapped
     *              in a {@link RuntimeExecutionException} and rethrow to the iterator consumer.
     *              If null, this signals a successful completion and the iterator will simply
     *              report the end of the batches/elements.
     */
    @Override public void complete(@Nullable Throwable error) { super.complete(error); }

    public static final class ClosedException extends IllegalStateException {
        public ClosedException(CallbackBatchIt<?> it) {
            super(it+" has been close()d before complete()");
        }
    }

    /* --- --- --- implementations --- --- --- */

    @Override protected void cleanup() {
        lock.lock();
        try {
            if (!ended)
                complete(new ClosedException(this));
        } finally { lock.unlock(); }
    }
}
