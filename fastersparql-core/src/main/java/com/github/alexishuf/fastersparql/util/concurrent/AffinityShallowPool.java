package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Runtime.getRuntime;

@SuppressWarnings("unchecked")
public class AffinityShallowPool<T> {
    private static final VarHandle P = MethodHandles.arrayElementVarHandle(Object[].class);
    private static final int THREAD_MASK
            = 32-Integer.numberOfLeadingZeros(8*getRuntime().availableProcessors()-1);
    private final T[] pool;

    public AffinityShallowPool(Class<T> cls) {
        pool = (T[])Array.newInstance(cls, THREAD_MASK+1);
    }

    public @Nullable T get() {
        int thread = (int) Thread.currentThread().threadId();
        T o =              (T)P.getAndSetAcquire(pool,  thread   &THREAD_MASK, null);
        if (o == null) o = (T)P.getAndSetAcquire(pool, (thread+1)&THREAD_MASK, null);
        return o;
    }

    public @Nullable T get(int thread) {
        T o =              (T)P.getAndSetAcquire(pool,  thread   &THREAD_MASK, null);
        if (o == null) o = (T)P.getAndSetAcquire(pool, (thread+1)&THREAD_MASK, null);
        return o;
    }

    @SuppressWarnings("UnusedReturnValue") public @Nullable T offer(@Nullable T o) {
        int thread = (int) Thread.currentThread().threadId();
        if (P.compareAndExchangeRelease(pool,  thread   &THREAD_MASK, null, o) == null)
            return null;
        if (P.compareAndExchangeRelease(pool, (thread+1)&THREAD_MASK, null, o) == null)
            return null;
        return o;
    }

    @SuppressWarnings("UnusedReturnValue") public @Nullable T offer(@Nullable T o, int thread) {
        if (P.compareAndExchangeRelease(pool,  thread   &THREAD_MASK, null, o) == null)
            return null;
        if (P.compareAndExchangeRelease(pool, (thread+1)&THREAD_MASK, null, o) == null)
            return null;
        return o;
    }
}
