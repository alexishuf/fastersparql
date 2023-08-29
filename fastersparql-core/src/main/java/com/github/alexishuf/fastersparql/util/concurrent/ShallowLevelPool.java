package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Integer.numberOfTrailingZeros;

@SuppressWarnings("unchecked")
public class ShallowLevelPool<T> {
    private static final VarHandle P = MethodHandles.arrayElementVarHandle(Object[].class);
    static final int STEAL_ATTEMPTS = 1;
    private static final int LEVELS = 32;
    private static final int L_MASK = LEVELS-1;
    private static final int L_LOG2 = numberOfTrailingZeros(LEVELS);
    static { assert Integer.bitCount(LEVELS) == 1 : "LEVELS not a power of 2"; }

    private final T[] pool;
    private final int threadMask;
    private final Class<T> itemClass;

    public ShallowLevelPool(Class<T> cls, int buckets) {
        buckets = Math.max(4,  1 << (32-Integer.numberOfLeadingZeros(buckets-1)));
        threadMask = buckets-1;
        itemClass = cls;
        pool = (T[]) Array.newInstance(cls, 32*buckets);
    }

    @SuppressWarnings("unused") public Class<T> itemClass() { return itemClass; }

    /**
     * Takes a previously {@code offer*()}ed object with capacity {@code >= capacity}.
     *
     * <p>Offers from threads that map to the same bucket will be queried first, but other
     * buckets will also be queried (stealing).</p>
     *
     * <p>If {@link #offerToNearest(Object, int)} was used on this pool, this method may return
     * an object with capacity {@code < capacity}.</p>
     *
     * @param capacity The minimum capacity for the returned object.
     * @return An object with at least {@code capacity} capacity or {@code null}.
     */
    public @Nullable T getAtLeast(int capacity) {
        int level = 31&(32-numberOfLeadingZeros(capacity-1));
        int thread = (int)Thread.currentThread().threadId();
        T o = null;
        for (int i = 0; i <= STEAL_ATTEMPTS; i++) {
            int b = ((thread + i) & threadMask) << L_LOG2;
            if ((o = (T)P.getAndSetRelease(pool, b+  level,            null)) != null) break;
            if ((o = (T)P.getAndSetRelease(pool, b+((level+1)&L_MASK), null)) != null) break;
        }
        return o;
    }

    /**
     * Offers an object {@code o} with given {@code capacity} to future {@link #getAtLeast(int)}.
     *
     * @param o the object which the caller wishes to cede ownership
     * @param capacity the capacity of the object, in the same unit as future
     *                 {@link #getAtLeast(int)} calls
     * @return {@code null} if ownership of the object was taken by the pool, {@code o} if the
     *         pool is full and ownership remains with caller.
     */
    public @Nullable T offerExact(T o, int capacity) {
        int l = 31&numberOfTrailingZeros(capacity);
        if (1<<l != capacity)
            return o; // not an exact capacity
        int thread = (int)Thread.currentThread().threadId();
        for (int i = 0; i <= STEAL_ATTEMPTS; i++) {
            int b = ((thread+i)&threadMask) << L_LOG2;
            if (P.compareAndExchangeRelease(pool, b+l, null, o) == null)
                return null;
        }
        return o;
    }

    /**
     * Offers an object {@code o} with given {@code capacity} to future {@link #getAtLeast(int)}.
     *
     * <p>Unlike {@link #offerExact(Object, int)}, this will accept {@code capacity} that is
     * not a power of two, and will treat such capacity as {@code 1 << floor(log2(capacity))}.
     * If a bucket is full for the floored capacity, an offer will be attempted to the next
     * smaller power of two capacity before this process is attempted on other thread buckets.</p>
     *
     * @param o the object which the caller wishes to cede ownership
     * @param capacity the capacity of the object, in the same unit as future
     *                 {@link #getAtLeast(int)} calls
     * @return {@code null} if ownership of the object was taken by the pool, {@code o} if the
     *         pool is full and ownership remains with caller.
     */
    public @Nullable T offerToFloor(T o, int capacity) {
        int l = 31&(31-numberOfLeadingZeros(capacity));
        int thread = (int)Thread.currentThread().threadId();
        for (int i = 0; i <= STEAL_ATTEMPTS; i++) {
            int b = ((thread+i)&threadMask) << L_LOG2;
            if (P.compareAndExchangeRelease(pool, b+  l,            null, o) == null)
                return null;
            if (P.compareAndExchangeRelease(pool, b+((l-1)&L_MASK), null, o) == null)
                return null;
        }
        return o;
    }

    /**
     * Equivalent to
     * <pre>{@code
     *    if (offerToFloor(o, capacity) == null)
     *        return null;
     *    return offerToExact(o, 1<<ceil(log2(capacity)));
     * }</pre>
     *
     * <p><strong>Important:</strong>This method may offer {@code o} as if it had a capacity
     * larger than {@code capacity}, thus a future {@link #getAtLeast(int)} call will return an
     * object with capacity smaller than requested. This method should only be used if {@code T}
     * can transparently grow its capacity as an {@link java.util.ArrayList} can.</p>
     *
     * @param o the object to offer for a future {@link #getAtLeast(int)}
     * @param capacity the capacity of {@code object}
     * @return {@code null} if the pool took ownership of {@code o}, {@code o} if the pool was full
     *          and the caller thus remains owner of {@code o}.
     */
    public @Nullable T offerToNearest(T o, int capacity) {
        int l = 31&(31-numberOfLeadingZeros(capacity));
        int thread = (int)Thread.currentThread().threadId();
        for (int i = 0; i <= STEAL_ATTEMPTS; i++) {
            int b = ((thread+i)&threadMask) << L_LOG2;
            if (P.compareAndExchangeRelease(pool, b+  l,            null, o) == null)
                return null;
            if (P.compareAndExchangeRelease(pool, b+((l-1)&L_MASK), null, o) == null)
                return null;
        }
        for (int i = 0; i <= STEAL_ATTEMPTS; i++) {
            int b = ((thread + i) & threadMask) << L_LOG2;
            if (P.compareAndExchangeRelease(pool, b+((l+1)&L_MASK), null, o) == null)
                return null;
        }
        return o;
    }
}
