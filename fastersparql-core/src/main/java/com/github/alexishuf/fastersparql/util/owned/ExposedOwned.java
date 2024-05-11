package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.OwnershipException;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner.Recycled;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

/**
 * In cases where a {@link Owned} implementation cannot extend {@link AbstractOwned}, this
 * interface provides implementations for most {@link Owned} methods, at the expense of exposing
 * some {@code unsafe*()} and {@code internalMark*()} methods.
 */
public interface ExposedOwned<O extends Owned<O>> extends Owned<O> {
    /**
     * <strong>Do not call</strong>
     */
    @Nullable Object unsafeInternalOwner0();

    /** <strong>Do not call</strong>
     *
     * <p>Implement as</p>
     *
     * <pre>{@code
     *     if (this.owner != expected)
     *         throw new OwnershipException(this, expected, this.owner)
     *     this.owner = newOwner;
     * }</pre>
     * */
    void unsafeUntracedExchangeOwner0(@Nullable Object expected,
                                      @Nullable Object newOwner) throws OwnershipException ;

    /** <strong>Do not call</strong>
     *
     * <p>Get a {@link OwnershipEvent} capturing the last ownership change event. This may
     * return a {@code null} pointer, even if {@link FSProperties#ownedTrace()}
     * {@code == true}</p>*/
    @Nullable OwnershipHistory unsafeInternalLastOwnershipHistory();

    @SuppressWarnings("unchecked") @Override
    default @This O requireOwner(Object expectedOwner) throws OwnershipException {
        if (ExposedOwnerSupport.MARK) {
            Object actual = unsafeInternalOwner0();
            if (actual != expectedOwner) {
                throw new OwnershipException(this, expectedOwner, actual,
                        unsafeInternalLastOwnershipHistory() );
            }
        }
        return (O) this;
    }

    @Override @Nullable default Object rootOwner() {
        Object owner = unsafeInternalOwner0();
        return owner instanceof Owned<?> o ? o.rootOwner() : owner;
    }

    @Override default boolean isOwnerOrNotMarking(Object owner) {
        if (ExposedOwnerSupport.MARK)
            return unsafeInternalOwner0() == owner;
        return true;
    }

    @Override default void requireAlive() throws OwnershipException {
        if (ExposedOwnerSupport.MARK) {
            Object owner = unsafeInternalOwner0();
            if (owner instanceof Recycled)
                throw new OwnershipException(this, owner, unsafeInternalLastOwnershipHistory());
        }
    }

    @Override default boolean isNotAliveAndMarking() {
        if (ExposedOwnerSupport.MARK)
            return unsafeInternalOwner0() instanceof Recycled;
        return false;
    }

    @Override default boolean isAliveOrNotMarking() {
        if (ExposedOwnerSupport.MARK)
            return !(unsafeInternalOwner0() instanceof Recycled);
        return true;
    }

    @Override default boolean isAliveAndMarking() {
        if (ExposedOwnerSupport.MARK)
            return !(unsafeInternalOwner0() instanceof Recycled);
        return false;
    }
}

class ExposedOwnerSupport {
    static final boolean MARK = FSProperties.ownedMark();
}
