package com.github.alexishuf.fastersparql.util.owned;

import com.github.alexishuf.fastersparql.batch.type.OwnershipException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

@SuppressWarnings("unchecked")
public interface SidecarOwned<O extends Owned<O>> extends Owned<O> {
    Sidecar<O> internalOwnedSidecar();

    @Override default @Nullable O recycle(Object currentOwner) {
        internalOwnedSidecar().recycle(currentOwner);
        return null;
    }

    @Override default @This O transferOwnership(Object currentOwner,
                                                Object newOwner) throws OwnershipException {
        internalOwnedSidecar().transferOwnership(currentOwner, newOwner);
        return (O)this;
    }

    @Override default Orphan<O> releaseOwnership(Object currentOwner) throws OwnershipException {
        internalOwnedSidecar().releaseOwnership(currentOwner);
        return (Orphan<O>)this;
    }

    @Override default boolean isNotAliveAndMarking() {
        return internalOwnedSidecar().isNotAliveAndMarking();
    }

    @Override default boolean isAliveOrNotMarking() {
        return internalOwnedSidecar().isAliveOrNotMarking();
    }

    @Override default boolean isAliveAndMarking() {
        return internalOwnedSidecar().isAliveAndMarking();
    }

    @Override default void requireAlive() throws OwnershipException {
        internalOwnedSidecar().requireAlive();
    }

    @Override default @Nullable Object rootOwner() {
        return internalOwnedSidecar().rootOwner();
    }

    @Override default boolean isOwnerOrNotMarking(Object owner) {
        return internalOwnedSidecar().isOwnerOrNotMarking(owner);
    }

    @Override @This default O requireOwner(Object owner) throws OwnershipException {
        internalOwnedSidecar().requireOwner(owner);
        return (O)this;
    }

    abstract class Sidecar<O extends Owned<O>> extends AbstractOwned<Sidecar<O>>
                                               implements Orphan<O>{
        protected final O managed;
        protected Sidecar(O managed) {this.managed = managed;}
        @Override public O takeOwnership(Object newOwner) {
            takeOwnership0(newOwner);
            return managed;
        }
    }
}
