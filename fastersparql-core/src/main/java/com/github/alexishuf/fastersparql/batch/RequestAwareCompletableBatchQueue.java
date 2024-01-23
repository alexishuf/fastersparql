package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;

/**
 * A {@link CompletableBatchQueue} that internally relies on asynchronously sending a request.
 * Since sending the request is asynchronous a {@link #cancel()} MAY arrive before or during
 * the sending of the request. This interface provides methods to serialize these two events
 * allowing the request to not be sent if {@link #cancel()} was called before and allowing
 * distinct implementations of {@link #cancel()} when it is called before or after the
 * request was sent.
 */
public interface RequestAwareCompletableBatchQueue<B extends Batch<B>> extends CompletableBatchQueue<B> {
    /** Blocks entry into {@link #cancel()} and into request-sending code. */
    void lockRequest();

    /** Undoes the previous {@link #lockRequest()}. */
    void unlockRequest();

    /**
     * If #cancel() has been called, return false. Else return {@code true} and set a flag
     * informing a later {@link #cancel()} that a request was already sent.
     *
     * @return {@code true} iff there is no previous {@link #cancel()} and thus a flag was set
     *          to inform a later {@link #cancel()} that the request was sent.
     * @throws IllegalStateException if this is not called within a
     *                               {@link #lockRequest()}-{@link #unlockRequest()} region held
     *                               by the calling thread..
     */
    boolean canSendRequest();
}
