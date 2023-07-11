package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Producer {
    /**
     * Idempotently notifies the producer it should start or resume offering batches.
     *
     * <p>The producer MUST stop computing (and eventually, delivering) batches once it observes
     * {@link AsyncEmitter#requested()} {@code <= 0}. This method should thus be called
     * everytime {@link AsyncEmitter#requested()} is incremented from a value {@code <= 0} to
     * a value {@code > 0}.</p>
     *
     * <p>The implementation should perform a quick and simple notification instead of waiting
     * for a batch, computing a batch or directly calling {@link AsyncEmitter#offer(Batch)}.</p>
     */
    void resume();

    /**
     * Notifies the producer it should stop producing batches forever. Once the producer
     * acknowledges this request (which may happen only after this call returns), it MUST invoke
     * {@link AsyncEmitter#producerTerminated()}.
     */
    void cancel();

    /**
     * Equivalent to {@code isComplete() || isCancelled() || error() != null}.
     *
     * @return whether this is completed, cancelled or failed.
     */
    boolean isTerminated();

    /**
     * Whether this producer exhausted its underlying data source and thus will not produce
     * more batches.
     *
     * @return {@code true} iff the producer completed without error or cancellation.
     */
    boolean isComplete();

    /**
     * Whether the producer stopped before reaching the end of its source due to
     * a {@link #cancel()}
     *
     * @return {@code true} iff {@link #cancel()} was called before natural completion.
     */
    boolean isCancelled();

    /**
     * The error that cause this producer to fail and stop producing batches.
     *
      * @return a non-null {@link Throwable} iff the producer stopped due to an error,
     *          {@code null} If the producer did not terminate, if it completed normally or if
     *          it was cancelled.
     */
    @Nullable Throwable error();
}
