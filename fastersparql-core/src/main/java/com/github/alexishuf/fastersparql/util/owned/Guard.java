package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.OwnershipException;
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

public sealed class Guard<O extends Owned<O>> implements AutoCloseable {
    protected @Nullable O owned;
    protected final Object owner;

    public Guard(Object owner) {
        this.owned = null;
        this.owner = owner;
    }

    public Guard(@Nullable O owned, Object owner) {
        this.owned = owned;
        this.owner = owner;
    }

    public Guard(@Nullable Orphan<O> orphan, Object owner) {
        this.owned = orphan == null ? null : orphan.takeOwnership(owner);
        this.owner = owner;
    }

    public final @PolyNull O set(@PolyNull O newOwned) {
        if (newOwned != null)
            newOwned.requireOwner(owner);
        O old = this.owned;
        if (old != newOwned && old != null)
            old.recycle(owner);
        this.owned = newOwned;
        return newOwned;
    }

    public final @PolyNull O set(@PolyNull Orphan<O> orphan) {
        return set(orphan == null ? null : orphan.takeOwnership(owner));
    }

    /**
     * Access the owned object held by this guard.
     * @throws OwnershipException if there is no object or if it is not owned by {@code owner}
     */
    public final O get() {
        O owned = this.owned;
        if (owned == null)
            throw new OwnershipException(null, owner, null, null);
        return owned.requireOwner(owner);
    }

    /**
     *  Removes the owned object from this guard and release it from {@code owner} ownership
     * @throws OwnershipException if there is no object (no {@code set()} or previous
     *                            {@code take()}/{@link #detach()}) or if the owned object is
     *                            somehow not owned by this guard's {@code owner} anymore.
     */
    public final Orphan<O> take() {
        O owned = this.owned;
        if (owned == null)
            throw new OwnershipException(null, owner, null, null);
        this.owned = null;
        return owned.releaseOwnership(owner);
    }

    /** Null-safe equivalent to {@link #take()}{@code .takeOwnership(owner)}. */
    public final @Nullable O detach() {
        O owned = this.owned;
        this.owned = null;
        return owned;
    }

    public final @Nullable Orphan<O> poll() {
        O owned = this.owned;
        if (owned == null)
            return null;
        this.owned = null;
        return owned.releaseOwnership(owner);
    }

    @Override public void close() {
        owned = Owned.safeRecycle(owned, owner);
    }

    public static sealed class BatchGuard<B extends Batch<B>> extends Guard<B> {
        public BatchGuard(Object owner) {super(owner);}
        public BatchGuard(@Nullable B owned, Object owner) {super(owned, owner);}
        public BatchGuard(Orphan<B> orphan, Object owner) {super(orphan, owner);}

        public B nextBatch(BIt<B> it) {
            return set(it.nextBatch(poll()));
        }

        public int      rows() { return owned == null ? 0 : owned.rows; }
        public int totalRows() { return owned == null ? 0 : owned.totalRows(); }
    }

    public static final class ItGuard<B extends Batch<B>> extends BatchGuard<B> {
        public final BIt<B> it;

        public ItGuard(Object owner, @Owning BIt<B> it) {
            super(owner);
            this.it = it;
        }

        public @Nullable B nextBatch() {return set(it.nextBatch(poll()));}

        public boolean advance() { return set(it.nextBatch(poll())) != null; }

        @Override public void close() {
            super.close();
            it.close();
        }
    }
}
