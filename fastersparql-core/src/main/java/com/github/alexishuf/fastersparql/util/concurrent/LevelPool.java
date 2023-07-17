package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.Map;
import java.util.WeakHashMap;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.System.identityHashCode;
import static java.lang.Thread.MIN_PRIORITY;
import static java.lang.Thread.onSpinWait;
import static java.util.Collections.synchronizedMap;

@SuppressWarnings("unchecked")
public final class LevelPool<T> {
    private static final Logger log = LoggerFactory.getLogger(LevelPool.class);

    /* --- --- --- atomic access --- --- --- */

    private static final VarHandle S = MethodHandles.arrayElementVarHandle(short[].class);

    /* --- --- --- constants --- --- --- */

    public static final int   DEF_TINY_LEVEL_CAPACITY = 256;
    public static final int  DEF_SMALL_LEVEL_CAPACITY = 1024;
    public static final int DEF_MEDIUM_LEVEL_CAPACITY = 512;
    public static final int  DEF_LARGE_LEVEL_CAPACITY = 256;
    public static final int   DEF_HUGE_LEVEL_CAPACITY = 8;

    public static final int   TINY_MAX_CAPACITY = 4;     // 0,    1,    2,  4
    public static final int  SMALL_MAX_CAPACITY = 256;   // 8,    16,   32, 64, 128, 256
    public static final int MEDIUM_MAX_CAPACITY = 2048;  // 512,  1024, 2048
    public static final int  LARGE_MAX_CAPACITY = 16384; // 4096, 8192, 16384
    public static final int   HUGE_MAX_CAPACITY = 1024*1024;  // 1M (1<<20)
    private static final int N_TINY_LEVELS   = 2 + numberOfTrailingZeros(TINY_MAX_CAPACITY);
    private static final int N_SMALL_LEVELS  = numberOfTrailingZeros(SMALL_MAX_CAPACITY)  - numberOfTrailingZeros(TINY_MAX_CAPACITY);
    private static final int N_MEDIUM_LEVELS = numberOfTrailingZeros(MEDIUM_MAX_CAPACITY) - numberOfTrailingZeros(SMALL_MAX_CAPACITY);
    private static final int N_LARGE_LEVELS  = numberOfTrailingZeros(LARGE_MAX_CAPACITY)  - numberOfTrailingZeros(MEDIUM_MAX_CAPACITY);
    private static final int N_HUGE_LEVELS   = numberOfTrailingZeros(HUGE_MAX_CAPACITY)   - numberOfTrailingZeros(LARGE_MAX_CAPACITY);
    private static final int S_SHIFT = numberOfTrailingZeros(64/4); // one "size" per cache line
    private static final short LOCKED  = Short.MIN_VALUE;
    private static final int MD_BASE_AND_CAP = (32+1)<<S_SHIFT;

    /* --- --- --- references cleanup --- --- --- */

    private static final int CLEAN_INTERVAL_MS = 2_000;
    private static final Map<LevelPool<?>, Boolean> pools = synchronizedMap(new WeakHashMap<>());

    private static void refCleanerThread() {
        while (true) {
            try {
                for (LevelPool<?> pool : pools.keySet()) {
                    try {
                        pool.cleanLeakyRefs();
                    } catch (Exception e) {
                        log.warn("{}.cleanLeakyRefs failed: ", pool, e);
                    }
                }
                //noinspection BusyWait
                Thread.sleep(CLEAN_INTERVAL_MS);
            } catch (InterruptedException e) {
                log.info("refCleanerThread exiting due to InterruptedException");
                break;
            } catch (Exception e) {
                log.warn("Unexpected error", e);
            }
        }
    }

    static {
        var t = new Thread(LevelPool::refCleanerThread, "LevelPool-refCleanerThread");
        t.setDaemon(true);
        t.setPriority(MIN_PRIORITY);
        t.start();
    }

    /* --- --- --- instance fields --- --- --- */

    private final T[] pool;
    private final short[] metadata;
    private final Class<T> cls;
    private final boolean forbidOfferToNearest;

    /* --- --- --- lifecycle --- --- --- */

    public LevelPool(Class<T> cls) {
        this(cls, DEF_TINY_LEVEL_CAPACITY, DEF_SMALL_LEVEL_CAPACITY, DEF_MEDIUM_LEVEL_CAPACITY,
                  DEF_LARGE_LEVEL_CAPACITY, DEF_HUGE_LEVEL_CAPACITY);
    }

    public LevelPool(Class<T> cls, int tinyLevelCap, int smallLevelCap, int mediumLevelCap,
                     int largeLevelCap, int hugeLevelCap) {
        this.cls = cls;
        this.forbidOfferToNearest = cls.isArray();
        this.pool = (T[]) Array.newInstance(cls, N_TINY_LEVELS   *   tinyLevelCap +
                                                  N_SMALL_LEVELS  *  smallLevelCap +
                                                  N_MEDIUM_LEVELS * mediumLevelCap +
                                                  N_LARGE_LEVELS  *  largeLevelCap +
                                                  N_HUGE_LEVELS   *   hugeLevelCap);
        this.metadata = new short[MD_BASE_AND_CAP+(33)*2];
        short base = 0;
        base = fillMetadata(0,                        TINY_MAX_CAPACITY, base, (short)tinyLevelCap);
        base = fillMetadata(  TINY_MAX_CAPACITY<<1,  SMALL_MAX_CAPACITY, base, (short)smallLevelCap);
        base = fillMetadata( SMALL_MAX_CAPACITY<<1, MEDIUM_MAX_CAPACITY, base, (short)mediumLevelCap);
        base = fillMetadata(MEDIUM_MAX_CAPACITY<<1,  LARGE_MAX_CAPACITY, base, (short)largeLevelCap);
        base = fillMetadata( LARGE_MAX_CAPACITY<<1,   HUGE_MAX_CAPACITY, base, (short)hugeLevelCap);
        assert base == pool.length : "base != pool.length";
    }

    private short fillMetadata(int firstCapacity, int lastCapacity, short base, short itemsPerLevel) {
        for (int cap = firstCapacity; cap <= lastCapacity; cap = Math.max(1, cap<<1), base += itemsPerLevel) {
            int level = 32 - numberOfLeadingZeros(cap);
            assert base + itemsPerLevel < Short.MAX_VALUE : "base overflows";
            metadata[MD_BASE_AND_CAP+(level<<1)  ] = base;
            metadata[MD_BASE_AND_CAP+(level<<1)+1] = itemsPerLevel;
        }
        return base;
    }

    /* --- --- --- core pool operations --- --- --- */

    public Class<T> itemClass() { return cls; }

    /**
     * Get an object previously {@link #offer(Object, int)}ed or
     * {@link #offerToNearest(Object, int)} that had its capacity reported as something that is
     * equals to or greater than {@code minRequiredSize}.
     *
     * <p><strong>Important:</strong> if {@link #offerToNearest(Object, int)} was previously used,
     * this call may return an object with capacity below {@code minRequiredSize}.</p>
     *
     * @param minRequiredSize minimum value for the length of returned array, if not null
     * @return {@code null} or an array {@code a} with {@code a.length >= minRequiredSize}
     */
    public @Nullable T getAtLeast(int minRequiredSize) {
        int level = minRequiredSize == 0 ? 0 : 33-numberOfLeadingZeros(minRequiredSize-1);
        return getFromLevel(level);
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted") boolean levelEmptyUnlocked(int level) {
        return (short)S.getOpaque(metadata, level<<S_SHIFT) == 0;
    }

    @Nullable T getFromLevel(int level) {
        short size, base = metadata[MD_BASE_AND_CAP + (level<<1)];
        while ((size = (short)S.getAndSetAcquire(metadata, level<<S_SHIFT, LOCKED)) == LOCKED)
            onSpinWait();
        try {
            return size > 0 ? pool[base + --size] : null;
            // do not write null, since doing so would invalidate cache lines of other consumers
        } finally {
            S.setRelease(metadata, level<<S_SHIFT, size);
        }
    }

    private void cleanLeakyRefs() {
        for (int l = 0; l < 33; l++) {
            short size;
            int base =      metadata[MD_BASE_AND_CAP+(l<<1)  ];
            int end  = base+metadata[MD_BASE_AND_CAP+(l<<1)+1];
            while ((size = (short)S.getAndSetAcquire(metadata, l<<S_SHIFT, LOCKED)) == LOCKED)
                onSpinWait();
            try {
                for (int i = base+size; i < end && pool[i] != null; i++)
                    pool[i] = null;
            } finally {
                S.setRelease(metadata, l<<S_SHIFT, size);
            }
        }
    }

    /**
     * Offers an array {@code o} with {@code o.length == capacity} to later callers of
     * {@link #getAtLeast(int)}.
     *
     * @param o An array {@code a}
     * @param capacity {@code a.length}
     * @return {@code null} if {@code o} got incorporated into the pool, else return {@code o}.
     *         This allows to nullify recycled fields, as in {@code this.x = INT.offer(x, x.length)}
     */
    public @Nullable T offer(T o, int capacity) {
        int level = 32 - numberOfLeadingZeros(capacity);
        int i = MD_BASE_AND_CAP+(level<<1), base = metadata[i], cap = metadata[i+1];
        short size;
        while ((size = (short)S.getAndSetAcquire(metadata, level<<S_SHIFT, LOCKED)) == LOCKED)
            onSpinWait();
        try {
            if (size < cap) {
                pool[base + size++] = o;
                o = null;
            }
        } finally {
            S.setRelease(metadata, level<<S_SHIFT, size);
        }
        return o;
    }

    @Nullable T offerToLevel(int level, T o) {
        int i = MD_BASE_AND_CAP+(level<<1), base = metadata[i], cap = metadata[i+1];
        short size;
        while ((size = (short)S.getAndSetAcquire(metadata, level<<S_SHIFT, LOCKED)) == LOCKED)
            onSpinWait();
        try {
            if (size < cap) {
                pool[base + size++] = o;
                o = null;
            }
        } finally {
            S.setRelease(metadata, level<<S_SHIFT, size);
        }
        return o;
    }

    /**
     * Offers {@code o} as if capacity where {@code 1<<floor(log2(capacity))}. If the pool is
     * full for that level and capacity is not a power of 2, attempts to offer as if capacity
     * where {@code 1<<ceil(log2(capacity))}.
     *
     * @param o an object whose ownership may be transferred to this pool so that it can be
     *          returned from a future {@link #getAtLeast(int)} or {@link #getAtLeast(int)} call.
     * @param capacity the capacity of {@code o}
     * @return {@code null} if ownership of {@code o} was taken by the pool, {@code o} if the
     *          caller remains owner of {@code o}. Recommended usage is
     *          {@code o = pool.offerToNearest(o, capacity)}, so that the caller reference will be
     *          set to null if it lost ownership.
     * @throws UnsupportedOperationException if {@link #itemClass()} is an array type.
     */
    public @Nullable T offerToNearest(T o, int capacity) {
        if (forbidOfferToNearest)
            throw new UnsupportedOperationException();
        int level = 32 - numberOfLeadingZeros(capacity);
        if (offerToLevel(level, o) == null)
            return null;
        return offerToLevel(31&(level+1), o);
    }

    /* --- --- --- Object methods --- --- --- */

    @Override public String toString() {
        int h = identityHashCode(this);
        return String.format("%s@%x[%s]", getClass().getSimpleName(), h, cls.getSimpleName());
    }
}
