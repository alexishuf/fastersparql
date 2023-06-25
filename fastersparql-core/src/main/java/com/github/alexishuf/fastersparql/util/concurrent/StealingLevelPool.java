package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Thread.currentThread;

/**
 * Tries a thread-local atomic for the level before delegating the get or offer operation to
 * a {@link LevelPool} instance.
 */
public class StealingLevelPool<T> {
    private static final VarHandle L = MethodHandles.arrayElementVarHandle(Object[].class);

    private final LevelPool<T> shared;
    private final T[] local;
    private final int threadMask;

    public StealingLevelPool(LevelPool<T> shared) {
        this(shared, Runtime.getRuntime().availableProcessors());
    }
    public StealingLevelPool(LevelPool<T> shared, int threads) {
        int safeThreads = 32 - numberOfLeadingZeros(threads - 1);
        this.threadMask = safeThreads-1;
        //noinspection unchecked
        this.local = (T[]) Array.newInstance(shared.itemClass(), safeThreads*32);
        this.shared = shared;
    }

    public @Nullable T getExact(int capacity) {
        return Integer.bitCount(capacity) != 1 ? null : getAtLeast(capacity);
    }

    public @Nullable T getAtLeast(int capacity) {
        if (capacity == 0)
            return null;
        return getFromLevel(32 - numberOfLeadingZeros(capacity-1));
    }

    @SuppressWarnings("unchecked")
    @Nullable T getFromLevel(int level) {
        int thread = (int)currentThread().threadId();
        T o = (T)L.getAndSetAcquire(local, (( thread   &threadMask)<<5)+level, null);
        if (o != null) return o;
        o =   (T)L.getAndSetAcquire(local, (((thread+1)&threadMask)<<5)+level, null);
        if (o != null) return o;
        return shared.getFromLevel(level);
    }

    public @Nullable T offer(T o, int capacity) {
        if (capacity == 0 || o == null)
            return null;
        int level = numberOfTrailingZeros(capacity);
        if (1<<level != capacity)
            return o;  // capacity not a power of 2
        // offer directly to pool if level is empty and unlocked. If o would still be offered
        // to local before, a get() could return null even with threadMask items pooled in local
        if (!shared.levelEmptyUnlocked(level)) {
            int t = (int)currentThread().threadId();
            if (L.compareAndExchangeRelease(local, (( t   &threadMask)<<5)+level, null, o) == null)
                return null;
            if (L.compareAndExchangeRelease(local, (((t-1)&threadMask)<<5)+level, null, o) == null)
                return null;
        }
        return shared.offer(o, capacity);
    }

    @Override public String toString() {
        return String.format("Local@%x(%s)", System.identityHashCode(this), shared);
    }
}
