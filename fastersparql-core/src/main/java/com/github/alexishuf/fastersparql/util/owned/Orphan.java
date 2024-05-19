package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.batch.type.OwnershipException;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.util.owned.OwnedSupport.ORPHAN_LOG;
import static com.github.alexishuf.fastersparql.util.owned.OwnedSupport.handleRecycleError;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.HANGMAN;

public interface Orphan<O extends Owned<O>>  {
    /**
     * Makes {@code newOwner} the newOwner of {@code this}
     * @param newOwner the new newOwner for this
     * @return the useful object, now owned by {@code newOwner}
     * @throws OwnershipException if {@code this} already has an newOwner or has been recycled with
     *                            {@link Owned#recycle(Object)}. This should never be thrown in
     *                            practice because a reference to an {@link Orphan} can only be
     *                            obtained from an un-owned, non-recycled object. IF this is
     *                            thrown, it indicates concurrent ownership changes.
     */
    O takeOwnership(Object newOwner);

    /**
     * Null-safe {@link #takeOwnership(Object)}.
     *
     * @param orphan A {@link Orphan}
     * @param newOwner the new owner for {@code orphan}, if {@code orphan != null}
     * @return {@code orphan.takeOwnership(newOwner)} or {@code null} if {@code orphan == null}
     */
    static <O extends Owned<O>> O takeOwnership(Orphan<O> orphan, Object newOwner) {
        return orphan == null ? null : orphan.takeOwnership(newOwner);
    }

    /**
     * Null-safe equivalent to {@link #takeOwnership(Object)} followed by
     * {@link Owned#recycle(Object)}.
     *
     * @param orphan a {@link Orphan} to take ownership and immediately recycle
     * @return {@code null}
     */
    static <O extends Owned<O>> @Nullable Orphan<O> recycle(@Nullable Orphan<O> orphan) {
        if (orphan != null)
            orphan.takeOwnership(HANGMAN).recycle(HANGMAN);
        return null;
    }

    static <O extends Owned<O>> @Nullable Orphan<O> safeRecycle(@Nullable Orphan<O> orphan) {
        if (orphan != null) {
            try {
                orphan.takeOwnership(HANGMAN).recycle(HANGMAN);
            } catch (Throwable t) {
                handleRecycleError(ORPHAN_LOG, "orphan", orphan, t);
            }
        }
        return null;
    }
}
