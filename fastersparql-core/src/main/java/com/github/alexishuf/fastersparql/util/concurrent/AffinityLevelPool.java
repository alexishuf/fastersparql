package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Thread.currentThread;

/**
 * Tries a thread-local atomic for the level before delegating the get or offer operation to
 * a {@link LevelPool} instance.
 */
public class AffinityLevelPool<T> {
    private static final VarHandle L = MethodHandles.arrayElementVarHandle(Object[].class);

    private final LevelPool<T> shared;
    private final T[] local;
    private final int threadMask;

    public AffinityLevelPool(LevelPool<T> shared) {
        this(shared, Runtime.getRuntime().availableProcessors());
    }
    public AffinityLevelPool(LevelPool<T> shared, int threads) {
        int safeThreads = 32 - numberOfLeadingZeros(threads - 1);
        this.threadMask = safeThreads-1;
        //noinspection unchecked
        this.local = (T[]) Array.newInstance(shared.itemClass(), safeThreads*32);
        this.shared = shared;
    }

    public @Nullable T getAtLeast(int capacity) {
        return getFromLevel(capacity == 0 ? 0 : 33 - numberOfLeadingZeros(capacity-1));
    }

    @SuppressWarnings("unchecked")
    @Nullable T getFromLevel(int level) {
        int bucket = (((int)currentThread().threadId()&threadMask)<<5)+level;
        T o = (T)L.getAndSetAcquire(local, bucket, null);
        if (o != null) return o;
        return shared.getFromLevel(level);
    }

    public @Nullable T offer(T o, int capacity) {
        int level = 32-numberOfLeadingZeros(capacity);
        // offer directly to pool if level is empty and unlocked. If o would still be offered
        // to local before, a get() could return null even with threadMask items pooled in local
        if (!shared.levelEmptyUnlocked(level)) {
            int bucket = (((int)currentThread().threadId()&threadMask)<<5)+level;
            if (L.compareAndExchangeRelease(local, bucket, null, o) == null)
                return null;
        }
        return shared.offerToLevel(level, o);
    }

    public @Nullable T offerToNearest(T o, int capacity) {
        int level = 32-numberOfLeadingZeros(capacity);
        // offer directly to pool if level is empty and unlocked. If o would still be offered
        // to local before, a get() could return null even with threadMask items pooled in local
        if (!shared.levelEmptyUnlocked(level)) {
            int bucket = (((int)currentThread().threadId()&threadMask)<<5)+level;
            if (L.compareAndExchangeRelease(local, bucket, null, o) == null)
                return null;
        }
        if (shared.offerToLevel(level, o) == null)
            return null;
        return shared.offerToLevel(31&(level+1), o);
    }

    @Override public String toString() {
        return String.format("%s@%x(%s)", getClass().getSimpleName().replace("LevelPool", ""),
                                          System.identityHashCode(this), shared);
    }
}
