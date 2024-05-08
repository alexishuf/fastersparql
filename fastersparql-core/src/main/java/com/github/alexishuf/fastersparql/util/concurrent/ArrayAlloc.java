package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.model.rope.SegmentRope;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import static com.github.alexishuf.fastersparql.batch.type.BatchType.PREFERRED_BATCH_TERMS;
import static com.github.alexishuf.fastersparql.util.concurrent.PoolStackSupport.THREADS;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.System.arraycopy;

@SuppressWarnings("unused")
public class ArrayAlloc<T> extends LevelAlloc<T> {

    /* --- --- --- empty array instances --- --- --- */
    public static final    int[] EMPTY_INT;
    public static final  short[] EMPTY_SHORT;
    public static final   long[] EMPTY_LONG;
    public static final Object[] EMPTY_OBJECT;
    public static final FinalSegmentRope[] EMPTY_F_SEG_ROPE;

    /* --- --- --- instances --- --- --- */
    public static final ArrayAlloc<             int[]> INT;
    public static final ArrayAlloc<           short[]> SHORT;
    public static final ArrayAlloc<            long[]> LONG;
    public static final ArrayAlloc<FinalSegmentRope[]> F_SEG_ROPE;
    public static final ArrayAlloc<          Object[]> OBJECT;

    private static final Runnable PRIME = new Runnable() {
        @Override public void run() {
            int batchLevel = len2level(PREFERRED_BATCH_TERMS);
            int maxLevel = len2level(Short.MAX_VALUE);
            for (var alloc : List.of(LONG, INT, F_SEG_ROPE)) {
                for (int level = 0; level <= batchLevel; level++)
                    alloc.primeLevel(level);
            }
            for (int level = 0; level <= batchLevel+1; level++)
                SHORT.primeLevel(level);
        }
        @Override public String toString() {return "ArrayAlloc.PRIME";}
    };
    static {
        int batchLevel = len2level(PREFERRED_BATCH_TERMS);
        assert batchLevel < 13;
        var intCap = new Capacities()
                .set(0, batchLevel, THREADS*256)
                .setSameBytesUsage(batchLevel+1, 15,
                        THREADS*128*(20+PREFERRED_BATCH_TERMS*4),
                        20, 4);
        var shortCap = new Capacities()
                .set(0, batchLevel+1, THREADS*256)
                .setSameBytesUsage(batchLevel+2, 15,
                        THREADS*128*(20+PREFERRED_BATCH_TERMS*2*2),
                        20, 2);
        var longCap = new Capacities()
                .set(15, 15, THREADS*32)
                .set(0, batchLevel, THREADS*256)
                .setSameBytesUsage(batchLevel+1, 14,
                        THREADS*128*(20+PREFERRED_BATCH_TERMS*8),
                        20, 8);

        INT        = new ArrayAlloc<>(   int[].class, "INTS",    4, intCap);
        SHORT      = new ArrayAlloc<>( short[].class, "SHORTS",  2, shortCap);
        LONG       = new ArrayAlloc<>(  long[].class, "LONGS",   8, longCap);
        OBJECT     = new ArrayAlloc<>(Object[].class, "OBJECTS", 4, longCap);
        F_SEG_ROPE = new ArrayAlloc<>(FinalSegmentRope[].class, "F_SEG_ROPES", 4, longCap);
        EMPTY_INT        = INT.createAtLeast(0);
        EMPTY_SHORT      = SHORT.createAtLeast(0);
        EMPTY_LONG       = LONG.createAtLeast(0);
        EMPTY_OBJECT     = OBJECT.createAtLeast(0);
        EMPTY_F_SEG_ROPE = F_SEG_ROPE.createAtLeast(0);
        Primer.INSTANCE.sched(PRIME);
    }

    /* --- --- --- lifecycle --- --- --- */
    
    public ArrayAlloc(Class<T> cls, String name, int bytesPerItem, Capacities capacities) {
        this(cls, name, bytesPerItem, capacities.array());
    }

    public ArrayAlloc(Class<T> cls, String name, int bytesPerItem, int[] capacities) {
        super(cls, name, 20, bytesPerItem, arrayFactory(cls), capacities);
        setZero(factory.apply(0));
    }

    @SuppressWarnings("unchecked")
    private static <T> IntFunction<T> arrayFactory(Class<T> arrayClass) {
        Class<?> compCls = arrayClass.componentType();
        T empty = (T) Array.newInstance(compCls, 0);
        return len -> len == 0 ? empty : (T)Array.newInstance(compCls, len);
    }

    /* --- --- --- methods --- --- --- */

    @Override public String journalName() {
        if (this ==      INT) return "ArrayAlloc.INT";
        if (this ==     LONG) return "ArrayAlloc.LONG";
        if (this == F_SEG_ROPE) return "ArrayAlloc.SEG_ROPE";
        if (this ==   OBJECT) return "ArrayAlloc.OBJECT";
        return super.journalName();
    }

    public T grow(T a, int capacity, int required) {
        T bigger = createAtLeast(required);
        //noinspection SuspiciousSystemArraycopy
        arraycopy(a, 0, bigger, 0, capacity);
        offerToNearest(a, capacity);
        return bigger;
    }

    /* --- --- --- utility static methods --- --- --- */

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
    public static FinalSegmentRope[] grow(FinalSegmentRope[] a, int size, int requiredSize) {
        FinalSegmentRope[] b = finalSegmentRopesAtLeast(requiredSize);
        arraycopy(a, 0, b, 0, size);
        return b;
    }
    public static FinalSegmentRope[] grow(FinalSegmentRope[] a, int requiredSize) {
        FinalSegmentRope[] b = finalSegmentRopesAtLeast(requiredSize);
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

    public static short[] shortsAtLeast(int len) {return SHORT.createAtLeast(len);}
    public static   int[]   intsAtLeast(int len) {return   INT.createAtLeast(len);}
    public static  long[]  longsAtLeast(int len) {return  LONG.createAtLeast(len);}
    public static FinalSegmentRope[] finalSegmentRopesAtLeast(int len) {return F_SEG_ROPE.createAtLeast(len);}
    public static      Object[]      objectsAtLeast(int len) {return   OBJECT.createAtLeast(len);}

    @SuppressWarnings("UnusedReturnValue")
    public static short[] recycleShorts(short[] a) {return SHORT.offer(a, a.length);}
    public static   int[]   recycleInts(  int[] a) {return   INT.offer(a, a.length);}
    public static  long[]  recycleLongs( long[] a) {return  LONG.offer(a, a.length);}
    @SuppressWarnings("UnusedReturnValue")
    public static FinalSegmentRope[] recycleSegmentRopes(FinalSegmentRope[] a) {
        return F_SEG_ROPE.offer(a, a.length);}
    public static Object[] recycleObjects(Object[] a) {return OBJECT.offer(a, a.length);}

    public static short[] recycleShortsAndGetEmpty(short[] a) {SHORT.offer(a, a.length); return EMPTY_SHORT; }
    public static   int[]   recycleIntsAndGetEmpty(  int[] a) {  INT.offer(a, a.length); return EMPTY_INT; }
    public static  long[]  recycleLongsAndGetEmpty( long[] a) { LONG.offer(a, a.length); return EMPTY_LONG; }
    public static FinalSegmentRope[] recycleSegmentRopesAndGetEmpty(FinalSegmentRope[] a) {F_SEG_ROPE.offer(a, a.length); return EMPTY_F_SEG_ROPE; }
    public static      Object[]      recycleObjectsAndGetEmpty(     Object[] a) {  OBJECT.offer(a, a.length); return EMPTY_OBJECT; }

    public static short[] cleanShortsAtLeast(int len) {
        int level = 32-numberOfLeadingZeros(len-1);
        var a = SHORT.pollAtLeast(len);
        if (a == null)
            return new short[1<<level];
        Arrays.fill(a, (short)0);
        return a;
    }
    public static int[] cleanIntsAtLeast(int len) {
        int level = 32-numberOfLeadingZeros(len-1);
        var a = INT.pollAtLeast(len);
        if (a == null)
            return new int[1<<level];
        Arrays.fill(a, 0);
        return a;
    }
    public static long[] cleanLongsAtLeast(int len) {
        int level = 32-numberOfLeadingZeros(len-1);
        var a = LONG.pollAtLeast(len);
        if (a == null)
            return new long[1<<level];
        Arrays.fill(a, 0L);
        return a;
    }
    public static FinalSegmentRope[] cleanSegmentRopesAtLeast(int len) {
        int level = 32-numberOfLeadingZeros(len-1);
        var a = F_SEG_ROPE.pollAtLeast(len);
        if (a == null)
            return new FinalSegmentRope[1<<level];
        Arrays.fill(a, null);
        return a;
    }
    public static Object[] cleanObjectsAtLeast(int len) {
        int level = 32-numberOfLeadingZeros(len-1);
        var a = OBJECT.pollAtLeast(len);
        if (a == null)
            return new SegmentRope[1<<level];
        Arrays.fill(a, null);
        return a;
    }


    public static short[] cleanShortsAtLeast(int len, short @Nullable [] a) {
        if (a != null && a.length < len)
            a = SHORT.offer(a, a.length);
        if (a == null) {
            int level = 32-numberOfLeadingZeros(len-1);
            if ((a = SHORT.pollFromLevel(level)) == null)
                return new short[INT.isLevelPooled(level) ? 1 << level : len];
        }
        Arrays.fill(a, (short)0);
        return a;
    }
    public static int[] cleanIntsAtLeast(int len, int @Nullable [] a) {
        if (a != null && a.length < len)
            a = INT.offer(a, a.length);
        if (a == null) {
            int level = 32-numberOfLeadingZeros(len-1);
            if ((a = INT.pollFromLevel(level)) == null)
                return new int[INT.isLevelPooled(level) ? 1 << level : len];
        }
        Arrays.fill(a, 0);
        return a;
    }
    public static long[] cleanLongsAtLeast(int len, long @Nullable [] a) {
        if (a != null && a.length < len)
            a = LONG.offer(a, a.length);
        if (a == null) {
            int level = 32-numberOfLeadingZeros(len-1);
            if ((a = LONG.pollFromLevel(level)) == null)
                return new long[LONG.isLevelPooled(level) ? 1 << level : len];
        }
        Arrays.fill(a, 0L);
        return a;
    }
    public static FinalSegmentRope[] cleanSegmentRopesAtLeast(int len, FinalSegmentRope @Nullable [] a) {
        if (a != null && a.length < len)
            a = F_SEG_ROPE.offer(a, a.length);
        if (a == null) {
            int level = 32-numberOfLeadingZeros(len-1);
            if ((a = F_SEG_ROPE.pollFromLevel(level)) == null)
                return new FinalSegmentRope[INT.isLevelPooled(level) ? 1 << level : len];
        }
        Arrays.fill(a, null);
        return a;
    }
    public static Object[] cleanObjectsAtLeast(int len, Object @Nullable [] a) {
        if (a != null && a.length < len)
            a = OBJECT.offer(a, a.length);
        if (a == null) {
            int level = 32-numberOfLeadingZeros(len-1);
            if ((a = OBJECT.pollFromLevel(level)) == null)
                return new SegmentRope[INT.isLevelPooled(level) ? 1 << level : len];
        }
        Arrays.fill(a, null);
        return a;
    }

    public static short[] shortsAtLeast(int minSize, short @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            SHORT.offer(a, a.length);
        }
        return SHORT.createAtLeast(minSize);
    }
    public static int[] intsAtLeast(int minSize, int @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            INT.offer(a, a.length);
        }
        return INT.createAtLeast(minSize);
    }
    public static long[] longsAtLeast(int minSize, long @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            LONG.offer(a, a.length);
        }
        return LONG.createAtLeast(minSize);
    }
    public static FinalSegmentRope[] finalSegmentRopesAtLeast(int minSize, FinalSegmentRope @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            F_SEG_ROPE.offer(a, a.length);
        }
        return F_SEG_ROPE.createAtLeast(minSize);
    }
    public static Object[] objectsAtLeast(int minSize, Object @Nullable[] a) {
        if (a != null) {
            if (a.length >= minSize) return a;
            OBJECT.offer(a, a.length);
        }
        return OBJECT.createAtLeast(minSize);
    }

    public static int[] copy(int[] a) {
        int[] b = INT.createAtLeast(a.length);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static long[] copy(long[] a) {
        long[] b = LONG.createAtLeast(a.length);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    public static FinalSegmentRope[] copy(FinalSegmentRope[] a) {
        FinalSegmentRope[] b = F_SEG_ROPE.createAtLeast(a.length);
        arraycopy(a, 0, b, 0, a.length);
        return b;
    }
}
