package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Thread.currentThread;

public class AffinityPool<T> implements LeakyPool {
    private static final int LOCKED = Integer.MIN_VALUE;
    private static final int L_SHIFT = Integer.numberOfTrailingZeros(64/4);
    private static final VarHandle L = MethodHandles.arrayElementVarHandle(Object[].class);
    private static final VarHandle S;
    static {
        try {
            S = MethodHandles.lookup().findVarHandle(AffinityPool.class, "plainSize", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final T[] shared, local;
    private final int mask, capacity;
    @SuppressWarnings("unused") private int plainSize;

    public AffinityPool(Class<T> cls, int capacity) {
        this(cls, capacity, Runtime.getRuntime().availableProcessors());
    }

    @SuppressWarnings("unchecked") public AffinityPool(Class<T> cls, int capacity, int threads) {
        int safeThreads = 1 << (32 - Integer.numberOfLeadingZeros(threads - 1));
        assert Integer.bitCount(safeThreads) == 1 && safeThreads >= threads;
        this.shared = (T[]) Array.newInstance(cls, capacity);
        this.local  = (T[]) Array.newInstance(cls, safeThreads<<L_SHIFT);
        this.capacity = capacity;
        this.mask = safeThreads-1;
        PoolCleaner.INSTANCE.monitor(this);
    }

    @Override public void cleanLeakyRefs() {
        T[] shared = this.shared;
        int size;
        while ((size = (int)S.getAndSetAcquire(this, LOCKED)) == LOCKED) Thread.onSpinWait();
        try {
            int mid = size;
            while (mid < shared.length && shared[mid] != null)
                ++mid;
            for (int i = size; i < mid; i++)
                shared[i] = null;
        } finally { S.setRelease(this, size); }
    }

    @SuppressWarnings("unchecked") public @Nullable T get() {
        int idx = (int)currentThread().threadId() & mask;
        T o = (T)L.getAndSetAcquire(local, idx << L_SHIFT, null);
        if (o != null)
            return o; // got from thread-local
        // try taking from shared LIFO
        int size;
        while ((size = (int)S.getAndSetAcquire(this, LOCKED)) == LOCKED) Thread.onSpinWait();
        if (size > 0) o = shared[--size];
        S.setRelease(this, size);
        if (o == null) // try stealing from neighbor thread
            o = (T)L.getAndSetAcquire(local, ((idx+1)&mask) << L_SHIFT, null);
        return o;
    }

    public @Nullable T offer(T o) {
        if (o == null)
            return null;
        int idx = (int)currentThread().threadId() & mask;
        // if shared is empty, offer to shared before. If local is always offered first, we
        // could have tens of pooled items and still return null from get().
        int size = (int)S.getAndSetAcquire(this, LOCKED);
        if (size != 0 && L.compareAndExchangeRelease(local, idx <<L_SHIFT, null, o) == null) {
            o = null; // store at thread-local
            if (size != LOCKED)
                S.setRelease(this, size);
        } else {
            for (; size == LOCKED; size = (int) S.getAndSetAcquire(this, LOCKED))
                Thread.onSpinWait();
            if (size < capacity) {
                shared[size++] = o;
                o = null; // store at shared LIFO
            }
            S.setRelease(this, size);
        }
        return o;
    }

    @Override public String toString() {
        return String.format("LocalStealingLIFOPool@%x{threads=%d, sharedCapacity=%d}",
                             System.identityHashCode(this), mask+1, capacity);
    }
}
