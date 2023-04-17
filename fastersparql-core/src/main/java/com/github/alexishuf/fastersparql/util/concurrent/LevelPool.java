package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.FS;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

public final class LevelPool<T> {
    private static final int LOCKED = 0x80000000;
    private static final VarHandle STATES = MethodHandles.arrayElementVarHandle(int[].class);

    private final T[] table;
    private final int width;
    private final int singletonWidth;
    private final int[] states = new int[32];

    public LevelPool(Class<T> cls, int capacity, int singletonCapacity) {
        width = capacity;
        singletonWidth = singletonCapacity;
        //noinspection unchecked
        table = (T[]) Array.newInstance(cls, 31*capacity + singletonCapacity);
        FS.addShutdownHook(this::purge);
    }

    public void purge() {
        for (int level = 0; level < states.length; level++) {
            int size = lockAndGetSize(level);
            for (int i = level*width, e = i+size; i < e; i++) table[i] = null;
            STATES.setRelease(states, level, 0);
        }
    }

    private int lockAndGetSize(int level) {
        int o = states[level], ex = o&~LOCKED;
        while ((o = (int)STATES.compareAndExchangeAcquire(states, level, ex, ex|LOCKED)) != ex) {
            ex = o &~LOCKED;
            Thread.onSpinWait();
        }
        return o;
    }

    /**
     * Possibly get a {@code T} previously given to {@link #offer(Object, int)}.
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
    public @Nullable T get(int capacity) {
        T o = null;
        int level = Integer.numberOfLeadingZeros(capacity|1), base = level*width;
        int size = lockAndGetSize(level);
        try {
            if (size > 0) o = table[base+--size];
        } finally {
            STATES.setRelease(states, level, size);
        }
        return o;
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
    public @Nullable T offer(@Nullable T o, int capacity) {
        if (o == null) return null;
        int level = Integer.numberOfLeadingZeros(capacity|1), base = level*width;
        int size = lockAndGetSize(level), cap = level == 31 ? singletonWidth : width;
        try {
//            new Exception("&o="+System.identityHashCode(o)).printStackTrace();
//            for (int i = 0; i < size; i++) {
//                if (table[base+i] == o)
//                    throw new AssertionError();
//            }
            if (size < cap) {
                table[base+size++] = o;
                o = null;
            }
        } finally {
            STATES.setRelease(states, level, size);
        }
        return o; // made null if assigned to arr
    }
}
