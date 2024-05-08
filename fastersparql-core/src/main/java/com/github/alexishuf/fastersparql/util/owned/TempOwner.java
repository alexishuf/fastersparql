package com.github.alexishuf.fastersparql.util.owned;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Steal ownership of a {@link Owned} object temporarily, for the duration of stack frame. Use as:
 *
 * <pre>{@code
 *     try (var tmp = new TmpOwner(owned)) {
 *         Orphan<O> orphan = tmp.releaseOwnership();
 *         doSomething(orphan);
 *         tmp.restoreOwnership(orphan);
 *     } // will enforce owned has the same owner it had before new TmpOwner()
 * }</pre>
 */
public class TempOwner<O extends Owned<O>> implements AutoCloseable {
    private final @Nullable Object oldOwner;
    private final O owned;
    private final Thread thread;

    public TempOwner(O owned) {
        this.thread = Thread.currentThread();
        this.owned = owned;
        if (owned instanceof AbstractOwned<?> ao)
            this.oldOwner = ao.owner();
        else if (owned instanceof ExposedOwned<?> eo)
            this.oldOwner = eo.unsafeInternalOwner0();
        else
            throw new UnsupportedOperationException("unsupported Owned<O> implementation");
        this.owned.transferOwnership(oldOwner, thread);
    }

    public Object tempOwner() { return thread; }

    public Orphan<O> releaseOwnership() {
        return owned.releaseOwnership(thread);
    }

    public void restoreOwnership(Orphan<O> orphan) {
        if (orphan != owned)
            throw new IllegalArgumentException("orphan != owned");
        orphan.takeOwnership(oldOwner);
    }

    @Override public void close() {
        if (owned.isOwner(thread))
            owned.transferOwnership(thread, oldOwner);
        else
            owned.requireOwner(oldOwner);
    }
}
