package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.OwnershipException;
import com.github.alexishuf.fastersparql.util.owned.LeakDetector.LeakState;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner.Recycled;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.GARBAGE;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public abstract class AbstractOwned<O extends AbstractOwned<O>> implements Owned<O> {
    private static final boolean MARK = FSProperties.ownedMark();
    private static final boolean TRACE = OwnershipHistory.ENABLED;
    private static final boolean DETECT_LEAKS = LeakDetector.ENABLED;

    private Object owner;
    private final @Nullable OwnershipHistory history;
    protected final @Nullable LeakState leakState;
    protected @Nullable String journalName;

    public AbstractOwned() {
        history = OwnershipHistory.createIfEnabled();
        if (LeakDetector.ENABLED)
            LeakDetector.register(this, leakState = makeLeakState(history));
        else
            leakState = null;
    }

    /**
     * The {@code final} class extending {@code O} must implement
     * {@link Orphan#takeOwnership(Object)} by delegating to this method.
     *
     * @param newOwner see {@link Orphan#takeOwnership(Object)}.
     */
    @SuppressWarnings("unchecked") protected final O takeOwnership0(Object newOwner) {
        Object actual = owner;
        if (MARK) {
            if (actual != null)
                throw new OwnershipException(this, null, actual, history);
            this.owner = newOwner;
            if (TRACE && history != null)
                history.taken(this, newOwner);
            if (DETECT_LEAKS && leakState != null)
                leakState.update(newOwner);
        } else {
            if (actual != null)
                throw new OwnershipException(this, null, actual, null);
            else if (newOwner instanceof Recycled)
                owner = newOwner;
        }
        return (O)this;
    }

    /**
     * Create a {@link LeakState} to handle leak detection and reporting for {@code this}.
     *
     * <p>This will only be called at most one time per {@link Owned} instance, from within the
     * constructor for {@link AbstractOwned} and therefore before the subclass constructors have
     * executed. This method will not be called if leak detection is disabled. Therefore,
     * implementations need not check {@link LeakDetector#ENABLED}.</p>
     *
     * @param history The {@link OwnershipHistory} for {@code this}
     * @return a {@link LeakState} for {@code this}
     */
    protected LeakState makeLeakState(@Nullable OwnershipHistory history) {
        return new LeakState(this, history);
    }


    @SuppressWarnings("unchecked") @Override
    public final @This O requireOwner(Object expectedOwner) {
        if (MARK) {
            Object actual = owner;
            if (actual != expectedOwner)
                throw new OwnershipException(this, expectedOwner, actual, history);
        }
        return (O)this;
    }

    @Override public boolean isOwnerOrNotMarking(Object owner) {
        if (MARK)
            return this.owner == owner;
        return true;
    }

    @Override public @Nullable Object rootOwner() {
        if (MARK) {
            Object owner = this.owner;
            return owner instanceof Owned<?> o ? o.rootOwner() : owner;
        }
        return null;
    }

    @Nullable Object owner() {return owner;}

    @Override public final void requireAlive() throws OwnershipException {
        if (MARK) {
            Object owner = this.owner;
            if (owner instanceof Recycled)
                throw new OwnershipException(this, owner, history);
        }
    }

    @Override public boolean isNotAliveAndMarking() {
        if (MARK)
            return owner instanceof Recycled;
        return false;
    }

    @Override public boolean isAliveOrNotMarking() {
        if (MARK)
            return !(owner instanceof Recycled);
        return true;
    }

    @Override public boolean isAliveAndMarking() {
        if (MARK)
            return !(owner instanceof Recycled);
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override public final Orphan<O> releaseOwnership(Object currentOwner) {
        Object actual = owner;
        if (MARK) {
            if (actual != currentOwner)
                throw new OwnershipException(this, currentOwner, actual, history);
            owner = null;
            if (TRACE && history != null)
                history.released(this);
            if (DETECT_LEAKS && leakState != null)
                leakState.update(null);
        } else if (actual != null) {
            if (actual != currentOwner)
                throw new OwnershipException(this, currentOwner, actual, null);
            owner = null;
        }
        return (Orphan<O>)this;
    }

    @SuppressWarnings("unchecked")
    @Override public @This O transferOwnership(Object currentOwner, Object newOwner) {
        Object actual = owner;
        if (MARK) {
            if (actual != currentOwner)
                throw new OwnershipException(this, currentOwner, actual, history);
            owner = newOwner;
            if (TRACE && history != null)
                history.transfer(this, newOwner);
            if (DETECT_LEAKS && leakState != null)
                leakState.update(newOwner);
        } else {
            if (actual != null && actual != currentOwner)
                throw new OwnershipException(this, currentOwner, actual, null);
            else if (newOwner instanceof Recycled)
                owner = newOwner;
            else if (actual != null)
                owner = null;
        }
        return (O)this;
    }

    protected final @Nullable O internalMarkRecycled(Object currentOwner) {
        Object actual = owner;
        if ((MARK || actual != null) && actual != currentOwner)
            throw new OwnershipException(this, currentOwner, actual, history);
        owner = RECYCLED;
        if (TRACE && history != null)
            history.recycled(this);
        if (DETECT_LEAKS && leakState != null)
            leakState.update(RECYCLED);
        return null;
    }

    protected @Nullable O internalMarkGarbage(Object currentOwner) {
        // Since the object intends to recycle/close its own resources, even if ownership
        // marking is disabled, owner = GARBAGE is still required to prevent double-free issues
        Object actual = owner;
        if ((MARK || actual != null) && actual != currentOwner)
            throw new OwnershipException(this, currentOwner, actual, history);
        owner = GARBAGE;
        if (TRACE && history != null)
            history.garbage(this);
        if (DETECT_LEAKS && leakState != null)
            leakState.update(GARBAGE);
        return null;
    }

    @Override public String toString() {return journalName();}

    @Override public String journalName() {
        if (journalName == null)
            journalName = OwnedSupport.makeJournalName(this);
        return journalName;
    }
}
