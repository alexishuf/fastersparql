package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;
import java.util.Map;
import java.util.WeakHashMap;

import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.System.identityHashCode;
import static java.lang.Thread.MIN_PRIORITY;
import static java.lang.Thread.onSpinWait;
import static java.util.Collections.synchronizedMap;

@SuppressWarnings("unchecked")
public class LevelPool<T> {
    private static final Logger log = LoggerFactory.getLogger(LevelPool.class);

    /* --- --- --- atomic access --- --- --- */

    private static final VarHandle S = MethodHandles.arrayElementVarHandle(short[].class);

    /* --- --- --- constants --- --- --- */

    private static final int       TINY_LEVEL_CAPACITY = 0;
    public  static final int  DEF_SMALL_LEVEL_CAPACITY = 1024;
    public  static final int DEF_MEDIUM_LEVEL_CAPACITY = 512;
    public  static final int  DEF_LARGE_LEVEL_CAPACITY = 256;
    public  static final int   DEF_HUGE_LEVEL_CAPACITY = 8;

    static final int      FIRST_CAPACITY = 4;
    static final int       LAST_CAPACITY = 1024*1024;  // 1M (1<<20)
    static final int  SMALL_MAX_CAPACITY = 256;   // 4,    8,    16,   32, 64, 128, 256
    static final int MEDIUM_MAX_CAPACITY = 2048;  // 512,  1024, 2048
    static final int  LARGE_MAX_CAPACITY = 16384; // 4096, 8192, 16384
    private static final int FIRST_LEVEL    = numberOfTrailingZeros(FIRST_CAPACITY);
    private static final int N_TINY_LEVELS   =  3; // 0, 1, 2
    private static final int N_SMALL_LEVELS  = numberOfTrailingZeros(SMALL_MAX_CAPACITY)  - (FIRST_LEVEL-1);
    private static final int N_MEDIUM_LEVELS = numberOfTrailingZeros(MEDIUM_MAX_CAPACITY) - numberOfTrailingZeros(SMALL_MAX_CAPACITY);
    private static final int N_LARGE_LEVELS  = numberOfTrailingZeros(LARGE_MAX_CAPACITY)  - numberOfTrailingZeros(MEDIUM_MAX_CAPACITY);
    private static final int N_HUGE_LEVELS   = numberOfTrailingZeros(LAST_CAPACITY)   - numberOfTrailingZeros(LARGE_MAX_CAPACITY);
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

    /* --- --- --- lifecycle --- --- --- */

    public LevelPool(Class<T> cls) {
        this(cls, DEF_SMALL_LEVEL_CAPACITY, DEF_MEDIUM_LEVEL_CAPACITY,
                  DEF_LARGE_LEVEL_CAPACITY, DEF_HUGE_LEVEL_CAPACITY);
    }

    @SuppressWarnings({"UnusedAssignment", "PointlessArithmeticExpression"})
    public LevelPool(Class<T> cls, int smallLevelCap, int mediumLevelCap,
                     int largeLevelCap, int hugeLevelCap) {
        this.cls = cls;
        this.pool = (T[]) Array.newInstance(cls, N_TINY_LEVELS*TINY_LEVEL_CAPACITY +
                                                  N_SMALL_LEVELS*smallLevelCap +
                                                  N_MEDIUM_LEVELS*mediumLevelCap +
                                                  N_LARGE_LEVELS*largeLevelCap +
                                                  N_HUGE_LEVELS*hugeLevelCap);
        this.metadata = new short[MD_BASE_AND_CAP+(32+1)*2];
        short l = (short) FIRST_LEVEL, base = 0;
        base = fillMetadata(l,                  N_TINY_LEVELS,   base, (short)TINY_LEVEL_CAPACITY);
        base = fillMetadata(l,                  N_SMALL_LEVELS,  base, (short)smallLevelCap);
        base = fillMetadata(l+=N_SMALL_LEVELS,  N_MEDIUM_LEVELS, base, (short)mediumLevelCap);
        base = fillMetadata(l+=N_MEDIUM_LEVELS, N_LARGE_LEVELS,  base, (short)largeLevelCap);
        base = fillMetadata(l+=N_LARGE_LEVELS,  N_HUGE_LEVELS,   base, (short)hugeLevelCap);
        assert base == pool.length : "base != shared.length";
    }

    private short fillMetadata(int firstLevel, int nLevels, short base, short itemsPerLevel) {
        assert base + nLevels*itemsPerLevel < Short.MAX_VALUE : "base will overflow";
        for (int l = firstLevel, e = l+nLevels; l < e; l++, base += itemsPerLevel) {
            metadata[MD_BASE_AND_CAP+(l<<1)  ] = base;
            metadata[MD_BASE_AND_CAP+(l<<1)+1] = itemsPerLevel;
        }
        return base;
    }

    /* --- --- --- core pool operations --- --- --- */

    public Class<T> itemClass() { return cls; }

    /**
     * Get a previously {@link #offer(Object, int)}ed array {@code a} with
     * {@code .length == capacity} or {@code null} if there is no such {@code a}.
     *
     * @param capacity the required value for {@code a.length}
     * @return the aforementioned array {@code a}
     */
    public @Nullable T getExact(int capacity) {
        int level = numberOfTrailingZeros(capacity);
        if (1<<level != capacity)
            return null; // not a supported capacity
        return getFromLevel(level);
    }

    /**
     * Similar to {@link #getExact(int)} but accepts {@code minRequiredSize} to be of a size not
     * managed by the pool and thus this method may return an array larger than
     * {@code minRequiredSize}.
     *
     * @param minRequiredSize minimum value for the length of returned array, if not null
     * @return {@code null} or an array {@code a} with {@code a.length >= minRequiredSize}
     */
    public @Nullable T getAtLeast(int minRequiredSize) {
        return getFromLevel(32 - Integer.numberOfLeadingZeros(minRequiredSize - 1));
    }

    boolean levelEmptyUnlocked(int level) {
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
        for (int l = FIRST_LEVEL, last = numberOfTrailingZeros(LAST_CAPACITY); l <= last; l++) {
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
     * {@link #getExact(int)}.
     *
     * @param o An array {@code a}
     * @param capacity {@code a.length}
     * @return {@code null} if {@code o} got incorporated into the pool, else return {@code o}.
     *         This allows to nullify recycled fields, as in {@code this.x = INT.offer(x, x.length)}
     */
    public @Nullable T offer(T o, int capacity) {
        int level = numberOfTrailingZeros(capacity);
        if (1<<level != capacity)
            return o; // not a supported capacity
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

    /* --- --- --- Object methods --- --- --- */

    @Override public String toString() {
        int h = identityHashCode(this);
        return String.format("%s@%x[%s]", getClass().getSimpleName(), h, cls.getSimpleName());
    }

}
