package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindReleasedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindStateException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;

public interface Rebindable {
    /**
     * Notifies that this instance may receive future {@link #rebind(BatchBinding)} calls,
     * allowing implementations to delay releasing internal resources until
     * {@link #rebindRelease()}.
     *
     * <p>If this is called after said internal resources have been released, the call will be
     * silently accepted but future {@link #rebind(BatchBinding)} calls may fail with
     * {@link RebindReleasedException}</p>
     *
     * <p><strong>Important</strong>: Each call of this method will require one future call
     * of {@link #rebindRelease()}, else system resources not managed by the JVM garbage
     * collector will never be released.</p>
     */
    void rebindAcquire();

    /**
     * Reverses the effect of one previous {@link #rebindAcquire()} call.
     *
     * <p>If there was only one {@link #rebindAcquire()} not yet undone, resources held by this
     * emitter pipeline may be released immediately or upon next termination. If there is no
     * {@link #rebindAcquire()} in effect this method will not throw an exception, but some
     * implementations may issue log messages to warn of the likely bug.</p>
     */
    void rebindRelease();

    /**
     * If this is an {@link Emitter}, move to an unstarted (pre-{@link Emitter#request(long)})
     * state and when requested, output solutions for {@code sparql.bound(binding)}, where
     * {@code sparql} is some query set during construction of the {@link Emitter}. The vars in
     * {@link Emitter#vars()} are not affected by this call, thus the produced batches must
     * include the values assigned in {@code bindings} if that column corresponded to a var in
     * {@link Emitter#vars()} that got bound in {@code sparql} by this call.
     *
     * <p>A {@code rebind()} is not cumulative: a second rebind will still refer to the same
     * {@code sparql} set in construction and not to the {@code sparql.bound(binding)} built by
     * the last {@code rebind}</p>
     *
     * <p>The given {@code binding} must never be null. If it is implementations are allowed to
     * fail with a {@link NullPointerException} or to treat it as a binding with no vars. If
     * {@code binding.vars()} is empty or does not intersects with any var of the query set in
     * when this emitter-like object was constructed, a {@code rebind} will behave as a simple
     * "reset". For such a reset, results are should be the same, but ordering can change and
     * the actual results may change if the underlying source had data changes.</p>
     *
     * <p>If the {@link Emitter} is live (i.e., between the first {@link Emitter#request(long)}
     * and the return of the last {@link Receiver#onBatch(Batch)}), calling {@code rebind} will
     * raise a {@link RebindStateException}. If a rebind is desired, the emitter must first be
     * {@link Emitter#cancel()}ed and the rebind should happen during or after
     * {@link Receiver#onComplete()}/{@link Receiver#onCancelled()}/
     * {@link Receiver#onError(Throwable)}.</p>
     *
     * <p>If this is an {@link Emitter}, It will behave analogously to the described above.
     * Critically, the number, and var-column mapping of batches is not affected by a rebind.
     * Non-{@link Emitter} implementors can also raise {@link RebindStateException} if they
     * implement a state diagram analogous to the {@link Emitter} states.</p>
     *
     * <p>Since this may be called after termination, {@link #rebindAcquire()} must be at least
     * once before the first rebind to avoid {@link RebindReleasedException} thrown by emitters
     * that released internal resources upon termination. To avoid resources leaking,
     * {@link #rebindRelease()} must be called sometime after the last rebind.</p>
     *
     * @param binding binding to be recursively applied to this and all upstream emitters
     * @throws RebindException if the operation cannot be performed due to reasons listed above.
     */
    void rebind(BatchBinding binding) throws RebindException;

    /**
     * Notifies that multiple calls to {@link #rebind(BatchBinding)} using a
     * {@link BatchBinding} to the same {@link Batch} but pointing to rows {@code >= binding.row}
     * will follow this call.
     *
     * <p>The effects of {@link #rebindPrefetchEnd(boolean)} are implicit at entry on this method.</p>
     *
     * <p>Implementations of this method may start a background task to process all rows
     * {@code >= binding.row} in {@code binding.batch} in advance, reducing latency of future
     * {@link #rebind(BatchBinding)} calls</p>
     *
     * @param binding a {@link BatchBinding} pointing to the first row in a batch whose
     *                subsequent rows will be used in future {@link #rebind(BatchBinding)} calls
     */
    default void rebindPrefetch(BatchBinding binding) {}

    /**
     * If there is a background task spawned by {@link #rebindPrefetch(BatchBinding)}, stop it.
     *
     * <p>If {@code sync == true}, this method will block until it is confirmed there is no
     * such task or until the task is successfully stopped. Else, this method will return ASAP
     * and the prefetching will eventually stop. Note that if {@code sync == false}, the
     * {@link BatchBinding} last passed to {@link #rebindPrefetch(BatchBinding)} and the then
     * attached {@link Batch} must not be modified until this method is called again with
     * {@code sync == true}</p>
     *
     * <p>There is no need to call this method before {@link #rebindPrefetch(BatchBinding)},
     * but this method <strong>MUST</strong> be called if no {@link #rebind(BatchBinding)} or
     * {@link #rebindPrefetch(BatchBinding)} calls will follow (to avoid wasteful processing)
     * and before {@code binding.batch} gets recycled (else the background tasks will process
     * garbage)</p>
     *
     * @param sync whether this call must wait until the background tasks have finished running
     *             and the {@link BatchBinding} given in the last
     *             {@link #rebindPrefetch(BatchBinding)} along with the then attached {@link Batch}
     *             can be safely mutated;
     */
    default void rebindPrefetchEnd(boolean sync)  {}

    /**
     * Set of vars that can be assigned via {@link #rebind(BatchBinding)}.
     */
    Vars bindableVars();
}
