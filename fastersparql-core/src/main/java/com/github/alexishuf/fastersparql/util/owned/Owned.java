package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.batch.type.OwnershipException;
import com.github.alexishuf.fastersparql.util.concurrent.JournalNamed;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import static com.github.alexishuf.fastersparql.util.owned.AbstractOwned.OWNED_LOG;

public interface Owned<O extends Owned<O>> extends JournalNamed {
    /**
     * Checks that {@code this} is owned by {@code owner}. If it is not, throws a
     * {@link OwnershipException}.
     *
     * @param owner The expected owner of this object
     * @throws OwnershipException if this has an owner other than {@code owner}, has no owner
     *                            or was {@link #recycle(Object)}ed.
     */
    @This O requireOwner(Object owner) throws OwnershipException;

    /**
     * Whether {@link #requireOwner(Object)} with {@code owner} would  <strong>NOT</strong>
     * throw a {@link OwnershipException}.
     *
     * @param owner the expected owner of {@code this}
     * @return {@code false} if {@code requireOwnership(owner)} would throw, else {@code true}.
     */
    boolean isOwner(Object owner);

    /**
     * Get an object which directly or indirectly owns {@code this} and has no owner itself.
     *
     * <p>Since the owner reported by this method may be an indirect owner,
     * {@link #isOwner(Object)} may return false when fed the output of this method.</p>
     *
     * @return an object, which ows {@code this} or indirectly owns the owner of {@code this}
     */
    @Nullable Object rootOwner();

    /**
     * If {@code owner} is a {@link Owned}, return {@link #rootOwner()}, else return {@code owner}.
     *
     * @param owner an owner of some {@link Owned} instance
     * @return {@code owner} or {@link #rootOwner()} of {@code owner}
     */
    static Object rootOwner(Object owner) {
        return owner instanceof Owned<?> o ? o.rootOwner() : owner;
    }

    /**
     * Checks that {@code this} is <strong>NOT</strong> either has no owner or has an owner
     * and {@link #recycle(Object)} has not yet been called.
     *
     * @throws OwnershipException if {@code this} is pooled or marked garbage
     */
    void requireAlive() throws OwnershipException ;

    /**
     * Whether {@link #requireAlive()} would <strong>NOT</strong> throw a {@link OwnershipException}.
     * @return {@code false} if {@link #requireAlive()} would throw, else {@code true}.
     */
    boolean isAlive();

    /**
     * Release ownership of this object, if it is owned by {@code currentOwner}.
     *
     * <p>If {@code this} has no owner, this call is a no-op.</p>
     *
     * @param currentOwner the current owner of {@code this}
     * @return A {@link Orphan}, which allows a future owner to take ownership using
     *         {@link Orphan#takeOwnership(Object)}.
     * @throws OwnershipException if {@code this} has no owner, was {@link #recycle(Object)}ed
     *         or has an owner and it is not {@code currentOwner}.
     */
    Orphan<O> releaseOwnership(Object currentOwner) throws OwnershipException;

    /**
     * Null-safe {@link #releaseOwnership(Object)}.
     *
     * @param owned {@code null} or a {@link Owned} owned by {@code currentOwner}
     * @param currentOwner the current owner of {@code owned}, if {@code owned != null}
     * @return A {@link Orphan} wrapping {@code owned} or {@code null} if {@code owned == null}
     */
    static <O extends Owned<O>> Orphan<O> releaseOwnership(Owned<O> owned, Object currentOwner) {
        return owned == null ? null : owned.releaseOwnership(currentOwner);
    }

    /**
     * Equivalent to {@link #releaseOwnership(Object)} with {@code currentOwner} followed by
     * {@link Orphan#takeOwnership(Object)} with {@code newOwner}, but generates a single stack
     * trace capture (if enabled) and only interacts with leak detection (if enabled) once.
     *
     * @param currentOwner the current owner of {@code this} (i.e., {@code isOwner(currentOwner) == true})
     * @param newOwner the new owner once this call returns
     * @return {@code this}
     * @throws OwnershipException if {@code currentOwner} is not the current owner for this.
     */
    @This O transferOwnership(Object currentOwner, Object newOwner) throws OwnershipException;

    /**
     * {@link #releaseOwnership(Object)} and subsequently recycles the object itself or
     * its contents.
     *
     * <p>The meaning of "recycle" is implementation-dependent: the object itself could be put
     * into a pool, resources ti holds may be pooled or resources (such as file descriptors
     * or memory) might be released/closed.</p>
     *
     * <p>This always return {@code null}, thus it can be used to clear fields and vars:</p>
     * <pre>{@code
     *     this.batch = this.batch.recycle(this); // recycles batch and drops the reference
     * }</pre>
     *
     * @param currentOwner the current owner of {@code this}.
     * @return {@code null}
     * @throws OwnershipException if {@code this} has no owner, has an owner other than
     *                            {@code owner} or was previously recycled.
     */
    @Nullable O recycle(Object currentOwner);

    /**
     * Null-safe {@link #recycle(Object)}.
     *
     * @param owned A {@link Owned} object, or {@code null}
     * @param currentOwner The current owner for {@code owned}, if {@code owned != null}.
     * @return {@code null}
     * @see #recycle(Object)
     */
    static <O> @Nullable O recycle(@Nullable Owned<?> owned, Object currentOwner) {
        if (owned != null)
            owned.recycle(currentOwner);
        return null;
    }

    /**
     * Null-safe and exception-safe {@link #recycle(Object)}.
     *
     * @param owned  object to recycle, if {@code null} nothing will be done
     * @param currentOwner current owner of {@code owned}
     * @return {@code null}
     */
    static <O> @Nullable O safeRecycle(@Nullable Owned<?> owned, Object currentOwner) {
        try {
            if (owned != null)
                owned.recycle(currentOwner);
        } catch (Throwable t) {
            OWNED_LOG.error("Error recycling {}", owned, t);
        }
        return null;
    }
}
