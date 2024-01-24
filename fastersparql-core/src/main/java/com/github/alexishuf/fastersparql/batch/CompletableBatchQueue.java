package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.sparql.results.ResultsParser;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface CompletableBatchQueue<B extends Batch<B>> extends BatchQueue<B> {
    /**
     * A queue becomes "terminated" after the first {@link #complete(Throwable)} or
     * {@link #cancel(boolean)} call. Only the first of such calls has an effect, with later calls being
     * ignored.
     *
     * @return whether this queue is cancelled or completed with or without error.
     */
    boolean isTerminated();

    /**
     * Whether this queue was marked as completed without error via a previous
     * {@code complete(null)} call while {@link #isTerminated()} was {@code false}.
     *
     * @return {@code true} iff the queue was completed without error
     */
    boolean isComplete();

    /**
     * Whether this queue was cancelled due to a {@link #cancel(boolean)} call while
     * {@link #isTerminated()} was {@code false}
     *
     * @return {@code true} iff {@link #cancel(boolean)} was called before natural completion.
     */
    boolean isCancelled();

    /**
     * The error passed to the {@link #complete(Throwable)} made while {@link #isTerminated()}
     * was false that made this queue enter the "failed" state.
     *
     * @return a non-null {@link Throwable} iff the producer stopped due to an error,
     *          {@code null} If the producer did not terminate, if it completed normally or if
     *          it was cancelled.
     */
    @Nullable Throwable error();

    /**
     * Puts the queue in a completed or failed (if {@code cause != null}) state.
     *
     * <p>Consumers will be able to consume all rows queued before this call. Once the queued rows
     * are depleted consumers will either observe that the queue completed successfully (there
     * are no more rows and there was no error) or will observe that the queue failed (no
     * more rows and there is an error, the {@code cause} given in this call.</p>
     *
     * @param cause if null, signals a successful completion, if {@code != null}, is the error
     *              that caused a completion before natural exhaustion of the producers.
     * @return {@code true} this queue had no previous {@code complete()} or {@link #cancel(boolean)}
     *         call
     */
    boolean complete(@Nullable Throwable cause);

    /**
     * Causes the queue to <strong>eventually</strong> terminate (reporting {@link #isCancelled()}
     * if no errors occur meanwhile). Once the termination is effected, {@link #offer(Batch)}
     * calls will fail with {@link CancelledException} (or with {@link TerminatedException} if
     * another error occurs before the cancellation becomes effective).
     *
     * @param ack whether this call is being made in acknowledgment of a cancel request. This is
     *            the case when this queue is fed by a {@link ResultsParser} and the underlying
     *            serialization has the notion cancel request and cancel acknowledgments. In such
     *            scenario the first {@code cancel(false)} causes a cancel request to be sent,
     *            and only when an acknowledgment arrives, {@code cancel(true)} will be called
     *            causing effective termination. If the cancel is spontaneous (e.g., from
     *            {@link Emitter#cancel()}), this parameter should be {@code false}
     *
     * @return {@code true} if this method call had an effect, {@code false} if this queue
     *         was already previously terminated due to exhaustion, error or a
     *         previous cancel.
     */
    boolean cancel(boolean ack);
}
