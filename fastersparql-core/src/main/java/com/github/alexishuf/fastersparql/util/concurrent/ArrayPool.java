package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;

import static com.github.alexishuf.fastersparql.util.concurrent.LevelPool.*;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.arraycopy;

public class ArrayPool<T> extends AffinityLevelPool<T> {
    /* --- --- --- instances --- --- --- */

    public static final ArrayPool<byte[]>  BYTE           = new ArrayPool<>(       byte[].class);
    public static final ArrayPool<int[]>   INT            = new ArrayPool<>(        int[].class);
    public static final ArrayPool<short[]> SHORT          = new ArrayPool<>(      short[].class);
    public static final ArrayPool<long[]>  LONG           = new ArrayPool<>(       long[].class);
    public static final ArrayPool<SegmentRope[]> SEG_ROPE = new ArrayPool<>(SegmentRope[].class);
    public static final ArrayPool<Object[]>      OBJECT   = new ArrayPool<>(     Object[].class);

    private static void prime(int firstCapacity, int lastCapacity, int n) {
        for (int capacity = firstCapacity; capacity <= lastCapacity; capacity <<= 1) {
            for (int i = 0; i < n; i++) {
                BYTE    .offer(new byte       [capacity], capacity);
                INT     .offer(new int        [capacity], capacity);
                SHORT   .offer(new short      [capacity], capacity);
                LONG    .offer(new long       [capacity], capacity);
                SEG_ROPE.offer(new SegmentRope[capacity], capacity);
                OBJECT  .offer(new Object     [capacity], capacity);
            }
        }
    }
    static {
        prime(1,                      TINY_MAX_CAPACITY,   Math.min(64, DEF_TINY_LEVEL_CAPACITY));
        prime(TINY_MAX_CAPACITY<<1,   SMALL_MAX_CAPACITY,  Math.min(64, DEF_SMALL_LEVEL_CAPACITY));
        prime(SMALL_MAX_CAPACITY<<1,  MEDIUM_MAX_CAPACITY, Math.min(32, DEF_MEDIUM_LEVEL_CAPACITY));
        prime(MEDIUM_MAX_CAPACITY<<1, LARGE_MAX_CAPACITY,  Math.min(4,  DEF_LARGE_LEVEL_CAPACITY));
    }

    /* --- --- --- lifecycle --- --- --- */
    
    private final Class<?> componentType;

    public ArrayPool(Class<T> cls) { this(new LevelPool<>(cls)); }
    public ArrayPool(LevelPool<T> shared) { this(shared, getRuntime().availableProcessors()); }
    public ArrayPool(LevelPool<T> shared, int threads) {
        super(shared, FIRST_HUGE_LEVEL, threads);
        this.componentType = shared.itemClass().componentType();
    }

    /* --- --- --- methods --- --- --- */

    @Override public String toString() {
        if (this ==     BYTE) return "LevelPool.BYTE";
        if (this ==      INT) return "LevelPool.INT";
        if (this ==     LONG) return "LevelPool.LONG";
        if (this == SEG_ROPE) return "LevelPool.SEG_ROPE";
        return super.toString();
    }

    public T grow(T a, int capacity, int required) {
        T bigger = getAtLeast(required);
        if (bigger == null) //noinspection unchecked
            bigger = (T) Array.newInstance(componentType, required);
        //noinspection SuspiciousSystemArraycopy
        arraycopy(a, 0, bigger, 0, capacity);
        offer(a, capacity);
        return bigger;
    }

    @SuppressWarnings("unused") public @NonNull T arrayAtLeast(int capacity) {
        T a = getAtLeast(capacity);
        //noinspection unchecked
        return a == null ? (T)Array.newInstance(componentType, capacity) : a;
    }

    /* --- --- --- empty array instances --- --- --- */
    public static final        byte[] EMPTY_BYTE     = new        byte[0];
    public static final         int[] EMPTY_INT      = new         int[0];
    public static final       short[] EMPTY_SHORT    = new       short[0];
    public static final        long[] EMPTY_LONG     = new        long[0];
    public static final SegmentRope[] EMPTY_SEG_ROPE = new SegmentRope[0];
    public static final Object     [] EMPTY_OBJECT   = new      Object[0];

    /* --- --- --- utility static methods --- --- --- */

    public static byte[] grow(byte[] a, int size, int requiredSize) {
        byte[] b = bytesAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, size);
        return b;
    }
    public static byte[] grow(byte[] a, int requiredSize) {
        byte[] b = bytesAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static int[] grow(int[] a, int size, int requiredSize) {
        int[] b = intsAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, size);
        return b;
    }
    public static int[] grow(int[] a, int requiredSize) {
        int[] b = intsAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static long[] grow(long[] a, int size, int requiredSize) {
        long[] b = longsAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, size);
        return b;
    }
    public static long[] grow(long[] a, int requiredSize) {
        long[] b = longsAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static SegmentRope[] grow(SegmentRope[] a, int size, int requiredSize) {
        SegmentRope[] b = segmentRopesAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, size);
        return b;
    }
    public static SegmentRope[] grow(SegmentRope[] a, int requiredSize) {
        SegmentRope[] b = segmentRopesAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static Object[] grow(Object[] a, int size, int requiredSize) {
        Object[] b = objectsAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, size);
        return b;
    }
    public static Object[] grow(Object[] a, int requiredSize) {
        Object[] b = objectsAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }

    public static byte[] bytesAtLeast(int minSize) {
        if (minSize == 0) return EMPTY_BYTE;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        byte[] a = BYTE.getFromLevel(level);
        return a == null ? new byte[1<<level] : a;
    }
    public static short[] shortsAtLeast(int minSize) {
        if (minSize == 0) return EMPTY_SHORT;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        short[] a = SHORT.getFromLevel(level);
        return a == null ? new short[1<<level] : a;
    }
    public static int[] intsAtLeast(int minSize) {
        if (minSize == 0) return EMPTY_INT;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        int[] a = INT.getFromLevel(level);
        return a == null ? new int[1<<level] : a;
    }
    public static long[] longsAtLeast(int minSize) {
        if (minSize == 0) return EMPTY_LONG;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        long[] a = LONG.getFromLevel(level);
        return a == null ? new long[1<<level] : a;
    }
    public static SegmentRope[] segmentRopesAtLeast(int minSize) {
        if (minSize == 0) return EMPTY_SEG_ROPE;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        SegmentRope[] a = SEG_ROPE.getFromLevel(level);
        return a == null ? new SegmentRope[1<<level] : a;
    }
    public static Object[] objectsAtLeast(int minSize) {
        if (minSize == 0) return EMPTY_OBJECT;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        Object[] a = OBJECT.getFromLevel(level);
        return a == null ? new Object[1<<level] : a;
    }

    @SuppressWarnings("unused") public static byte[] bytesAtLeastUpcycle(int minSize) {
        if (minSize == 0) return EMPTY_BYTE;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        byte[] a = BYTE.getFromLevel(level);
        if (a != null && a.length > 1<<level && BYTE.offer(a, a.length) == null)
            a = BYTE.getFromLevel(level);
        return a == null ? new byte[1<<level] : a;
    }
    @SuppressWarnings("unused") public static short[] shortsAtLeastUpcycle(int minSize) {
        if (minSize == 0) return EMPTY_SHORT;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        short[] a = SHORT.getFromLevel(level);
        if (a != null && a.length > 1<<level && SHORT.offer(a, a.length) == null)
            a = SHORT.getFromLevel(level);
        return a == null ? new short[1<<level] : a;
    }
    @SuppressWarnings("unused") public static int[] intsAtLeastUpcycle(int minSize) {
        if (minSize == 0) return EMPTY_INT;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        int[] a = INT.getFromLevel(level);
        if (a != null && a.length > 1<<level && INT.offer(a, a.length) == null)
            a = INT.getFromLevel(level);
        return a == null ? new int[1<<level] : a;
    }
    public static long[] longsAtLeastUpcycle(int minSize) {
        if (minSize == 0) return EMPTY_LONG;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        long[] a = LONG.getFromLevel(level);
        if (a != null && a.length > 1<<level && LONG.offer(a, a.length) == null)
            a = LONG.getFromLevel(level);
        return a == null ? new long[1<<level] : a;
    }
    public static SegmentRope[] segmentRopesAtLeastUpcycle(int minSize) {
        if (minSize == 0) return EMPTY_SEG_ROPE;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        SegmentRope[] a = SEG_ROPE.getFromLevel(level);
        if (a != null && a.length > 1<<level && SEG_ROPE.offer(a, a.length) == null)
            a = SEG_ROPE.getFromLevel(level);
        return a == null ? new SegmentRope[1<<level] : a;
    }
    @SuppressWarnings("unused") public static Object[] objectsAtLeastUpcycle(int minSize) {
        if (minSize == 0) return EMPTY_OBJECT;
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        Object[] a = OBJECT.getFromLevel(level);
        if (a != null && a.length > 1<<level && OBJECT.offer(a, a.length) == null)
            a = OBJECT.getFromLevel(level);
        return a == null ? new Object[1<<level] : a;
    }

    public static byte[] bytesAtLeast(int minSize, byte @Nullable [] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            BYTE.offer(a, a.length);
        } else if (minSize == 0) {
            return EMPTY_BYTE;
        }
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        return (a = BYTE.getFromLevel(level)) == null ? new byte[1<<level] : a;
    }
    @SuppressWarnings("unused") public static short[] shortsAtLeast(int minSize, short @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            SHORT.offer(a, a.length);
        } else if (minSize == 0) {
            return EMPTY_SHORT;
        }
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        return (a = SHORT.getFromLevel(level)) == null ? new short[1<<level] : a;
    }
    public static int[] intsAtLeast(int minSize, int @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            INT.offer(a, a.length);
        } else if (minSize == 0) {
            return EMPTY_INT;
        }
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        return (a = INT.getFromLevel(level)) == null ? new int[1<<level] : a;
    }
    public static long[] longsAtLeast(int minSize, long @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            LONG.offer(a, a.length);
        } else if (minSize == 0) {
            return EMPTY_LONG;
        }
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        return (a = LONG.getFromLevel(level)) == null ? new long[1<<level] : a;
    }
    public static SegmentRope[] segmentRopesAtLeast(int minSize, SegmentRope @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            SEG_ROPE.offer(a, a.length);
        } else if (minSize == 0) {
            return EMPTY_SEG_ROPE;
        }
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        return (a = SEG_ROPE.getFromLevel(level)) == null ? new SegmentRope[1<<level] : a;
    }
    @SuppressWarnings("unused") public static Object[] objectsAtLeast(int minSize, Object @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            OBJECT.offer(a, a.length);
        } else if (minSize == 0) {
            return EMPTY_OBJECT;
        }
        int level = 33 - Integer.numberOfLeadingZeros(minSize-1);
        return (a = OBJECT.getFromLevel(level)) == null ? new Object[1<<level] : a;
    }

    public static byte[] copy(byte[] a) {
        byte[] b = bytesAtLeast(a.length);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static int[] copy(int[] a) {
        int[] b = intsAtLeast(a.length);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static long[] copy(long[] a) {
        long[] b = longsAtLeast(a.length);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static SegmentRope[] copy(SegmentRope[] a) {
        SegmentRope[] b = segmentRopesAtLeast(a.length);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
}
