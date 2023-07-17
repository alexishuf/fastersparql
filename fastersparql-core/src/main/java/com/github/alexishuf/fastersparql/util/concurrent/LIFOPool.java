package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.Arrays;

import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.lang.invoke.MethodHandles.lookup;

public final class LIFOPool<T> implements LeakyPool {
    private static final int LOCKED = Integer.MIN_VALUE;
    private static final VarHandle S;
    static {
        try {
            S = lookup().findVarHandle(LIFOPool.class, "plainSize", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Class<T> cls;
    private final T[] recycled;
    @SuppressWarnings({"unused"}) // lock is accessed through LOCK
    private int plainSize;

    public LIFOPool(Class<T> cls, int capacity) {
        //noinspection unchecked
        recycled = (T[]) Array.newInstance(cls, capacity);
        this.cls = cls;
    }

    public Class<T> itemClass() { return cls; }

    @Override public void cleanLeakyRefs() {
        int size;
        while ((size = (int)S.getAndSetAcquire(this, LOCKED)) == LOCKED) Thread.onSpinWait();
        try {
            Arrays.fill(recycled, size, recycled.length, null);
        } finally {
            S.setRelease(this, size);
        }
    }

    /**
     * Possibly get a {@code T} previously given to {@link LIFOPool#offer(Object)}.
     *
     * <p>If non-null and on a single-threaded program, this will return the last
     * {@code offer()} not yet returned by a {@code get()}. Under concurrency of {@code get()}s,
     * {@code offer()}s or {@code get()}s and {@code offer()}s, LIFO order is not
     * guaranteed and there is no guarantee that a {@code offer()} will ever be returned in a
     * future {@code get()} call. The guarantee that a {@code offer()} will only be visible
     * through at most one {@code get()} remains on all combinations of concurrent calls.</p>
     *
     * @return a {@code T} previously {@code offer()}ed and not yet {@code get()}ed.
     */
    public @Nullable T get() {
        int size;
        while ((size = (int)S.getAndSetAcquire(this, LOCKED)) == LOCKED) Thread.onSpinWait();
        try {
            if (size == 0) return null;
            return recycled[--size];
        } finally {
            S.setRelease(this, size);
        }
    }

    /**
     * Offers an instance of {@code T} to be returned by at most one future {@code get()} call.
     *
     * @param o an object to be recycled.
     * @return {@code o} iff the caller retains ownership, {@code null} if ownership has been
     *         taken by the pool. If this returns {@code null}, the caller MUST not read/write to
     *         {@code o}, since another thread  may have concurrently acquired ownership of it
     *         through {@code get()}.
     */
    public @Nullable T offer(@Nullable T o) {
        if (o == null) return null;
        int size;
        while ((size = (int)S.getAndSetAcquire(this, LOCKED)) == LOCKED) Thread.onSpinWait();
        try {
            if (size == recycled.length)
                return o;
//            new Exception("&o="+System.identityHashCode(o)+", thread="+Thread.currentThread()).printStackTrace(System.out);
//            for (int i = 0; i < size; i++) {
//                if (recycled[i] == o)
//                    throw new AssertionError("double free");
//            }
            recycled[size++] = o;
            return null;
        } finally {
            S.setRelease(this, size);
        }
    }

    @Override public String toString() {
        return format("LIFOPool@%x{cap=%d, cls=%s}",
                      identityHashCode(this), recycled.length, cls.getSimpleName());
    }
}
