package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.index.qual.LessThan;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.concurrent.PoolStackSupport.*;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.lang.Thread.currentThread;

public class LevelAlloc<T> implements LeakyPool, StatsPool, JournalNamed {
    private static final int MD_LEVEL_WIDTH  = 4;
    private static final int MD_LEVEL_SHIFT  = numberOfTrailingZeros(MD_LEVEL_WIDTH);
    private static final int MD_BASE         = 128/4;
    private static final int MD_THREAD_WIDTH = 33*MD_LEVEL_WIDTH;
    private static final int MD_SHARED_BASE  = MD_BASE + THREADS*MD_THREAD_WIDTH;
    private static final int POOL_BASE = 128/4;

    /**
     * this encodes state for {@code 1 + threads*levels} LIFO stacks. Each stack corresponds to
     * one length level {@code >=0 0 && level <= 32} of one thread, except for the
     * last 33 stacks, that correspond to the levels shared by all threads.
     *
     * <pre>
     *     thread(0), level(0)      |  thread(0), level (1)     | ... | thread(n), level(0)
     *     lock |size | begin | cap | lock | size | begin | cap | ... | lock | size | begin ...
     * </pre>
     */
    private final int[] md;
    private final T[] pool;
    private final int pooledLevels;
    protected final IntFunction<T> factory;
    private final String journalName;
    private final int fixedBytesPerObject;
    private final int bytesPerItemInObject;
    private @MonotonicNonNull T zero;

    public static final class Capacities {
        private final int[] capacities = new int[33];

        public @This Capacities set(@NonNegative @LessThan("#2") int first,
                                    @NonNegative int last, @NonNegative int capacity) {
            if (last < first)
                throw new IllegalArgumentException("last < first ");
            Arrays.fill(capacities, first, last+1, capacity);
            return this;
        }

        public @This Capacities
        setSameBytesUsage(@NonNegative @LessThan("#2") int first, @NonNegative int last,
                          @NonNegative int bytes, @Positive int fixedObjectOverhead,
                          @Positive int bytesPerItemInObject) {
            if (last < first)
                throw new IllegalArgumentException("last < first ");
            if (bytes < fixedObjectOverhead)
                throw new IllegalArgumentException("bytes < fixedObjectOverhead");
            if (bytes < bytesPerItemInObject)
                throw new IllegalArgumentException("bytes < bytesPerItemInObject");
            for (int i = first; i <= last; i++) {
                int len = i == 0 ? 0 : 1<<i;
                int objCost = fixedObjectOverhead + len * bytesPerItemInObject;
                capacities[i] = Math.max(1, bytes/objCost);
            }
            return this;
        }

        public int[] array() {return capacities;}
    }

    public LevelAlloc(Class<T> cls, @Nullable String name, int fixedObjectOverhead,
                      int bytesPerItemInObject, IntFunction<T> factory, Capacities capacities) {
        this(cls, name, fixedObjectOverhead, bytesPerItemInObject, factory, capacities.capacities);
    }

    @SuppressWarnings("unchecked")
    public LevelAlloc(Class<T> cls, @Nullable String name, int fixedBytesPerObject,
                      int bytesPerItemInObject, IntFunction<T> factory, int[] capacities) {
        if (capacities.length != 33)
            throw new IllegalArgumentException("capacities must be a int[33]");
        this.fixedBytesPerObject = fixedBytesPerObject;
        this.bytesPerItemInObject = bytesPerItemInObject;
        this.factory = Objects.requireNonNull(factory);
        Objects.requireNonNull(cls);
        this.journalName = name != null ? name
                         : format("Alloc(%s)@%x", cls.getSimpleName(), identityHashCode(this));
        this.md = new int[MD_SHARED_BASE+MD_THREAD_WIDTH];
        int pooledLevels = 0;
        for (int level = 0; level <= 32; level++) {
            if (capacities[level] > 0) pooledLevels |= 1<<level;
        }
        this.pooledLevels = pooledLevels;
        int poolSize = POOL_BASE;
        for (int thread = 0; thread < THREADS; thread++) {
            int threadStart = poolSize;
            for (int level = 0; level <= 32; level++) {
                int mdb = mdb(thread) + (level<<MD_LEVEL_SHIFT);
                int cap = localCapacity(capacities[level]);
                assert cap <= THREAD_WIDTH;
                md[mdb+MD_SIZE ] = 0;
                md[mdb+MD_BEGIN] = poolSize;
                md[mdb+MD_CAP  ] = cap;
                poolSize        += cap;
            }
            poolSize = Math.max(poolSize, threadStart+THREAD_WIDTH);
        }
        for (int level = 0; level <= 32; level++) {
            int mdb = MD_SHARED_BASE + (level<<MD_LEVEL_SHIFT);
            int cap = capacities[level];
            md[mdb+MD_SIZE ] = 0;
            md[mdb+MD_BEGIN] = poolSize;
            md[mdb+MD_CAP  ] = cap;
            poolSize        += cap;
        }
        this.pool = (T[])Array.newInstance(cls, poolSize);
        checkInitialized(md, MD_BASE, MD_LEVEL_WIDTH, pool);
        PoolCleaner.monitor(this);
        PoolStats.monitor(this);
    }

    private static int mdb(int thread) {
        return MD_BASE + (thread&THREADS_MASK)*MD_THREAD_WIDTH;
    }

    @Override public String toString() {return journalName();}
    @Override public String journalName() {return journalName;}

    public StringBuilder dump(StringBuilder sb) {
        sb.append(name()).append("{");
        for (int level = 0; level <= 32; level++) {
            if (isLevelPooled(level)) {
                int lBase = level<<MD_LEVEL_SHIFT;
                sb.append("\n  ").append(level);
                sb.append(": sharedCap=").append(md[MD_SHARED_BASE+lBase+MD_CAP]);
                sb.append(" sharedSize=").append(md[MD_SHARED_BASE+lBase+MD_SIZE]);
                sb.append(" localCap=").append(md[MD_BASE+lBase+MD_CAP]);
                sb.append(" localSizes=[");
                for (int thread = 0; thread < THREADS; thread++)
                    sb.append(md[mdb(thread)+lBase+MD_SIZE]).append(' ');
                sb.setLength(sb.length()-1);
                sb.append(']');
            }
        }
        return sb.append("\n}");
    }

    @SuppressWarnings("unused") // used only for testing & debug
    public String dump() { return dump(new StringBuilder()).toString(); }

    @Override public String name() {return journalName;}

    @Override public int sharedObjects() {
        int n = 0;
        for (int level = 0; level <= 32; level++)
            n += md[MD_SHARED_BASE+(level<<MD_LEVEL_SHIFT)+MD_SIZE];
        return n;
    }

    @Override public int sharedBytes() {
        int n = 0;
        for (int level = 0; level <= 32; level++) {
            int objects = md[MD_SHARED_BASE + (level << MD_LEVEL_SHIFT) + MD_SIZE];
            int len = level == 32 ? 0 : 1<<level;
            n += objects*fixedBytesPerObject + objects*len*bytesPerItemInObject;
        }
        return n;
    }

    @Override public int localObjects() {
        int n = 0;
        for (int thread = 0; thread < THREADS; thread++) {
            for (int level = 0; level <= 32; level++)
                n += md[mdb(thread)+(level<<MD_LEVEL_SHIFT)+MD_SIZE];
        }
        return n;
    }

    @Override public int localBytes() {
        int n = 0;
        for (int thread = 0; thread < THREADS; thread++) {
            for (int level = 0; level <= 32; level++) {
                int objects = md[mdb(thread) + (level << MD_LEVEL_SHIFT) + MD_SIZE];
                int len = level == 32 ? 0 : 1<<level;
                n += objects*fixedBytesPerObject + objects*len*bytesPerItemInObject;
            }
        }
        return n;
    }

    public static int len2level(int len) {return 32 - numberOfLeadingZeros(len-1);}
    public static int level2len(int level) {return level == 32 ? 0 : 1<<level;}

    /**
     * Whether the given level, obtained from {@link #len2level(int)} is pooled, i.e., it has a
     * non-zero capacity.
     *
     * @param level the level
     * @return whether offers at this level will be pooled
     */
    public boolean isLevelPooled(int level) {return (pooledLevels&(1<<level)) != 0;}

    /** Equivalent to {@link #isLevelPooled(int)} with {@link #len2level(int)} of {@code len}. */
    @SuppressWarnings("unused") public boolean isLenPooled(int len) {
        return (pooledLevels&(1<<len2level(len))) != 0;
    }

    /* --- --- --- poll --- --- --- */

    /** Equivalent to {@link #pollAtLeast(int, int)} */
    public final @Nullable T pollAtLeast(@NonNegative int len) {
        return pollFromLevel(len2level(len));
    }

    /** Equivalent to {@link #pollFromLevel(int, int)}, but computes {@code level}
     * from {@code len}. */
    public final @Nullable T pollAtLeast(int threadId, @NonNegative int len) {
        return pollFromLevel(threadId, len2level(len));
    }

    /** Equivalent to {@link #pollFromLevel(int, int)}. */
    public final @Nullable T pollFromLevel(@NonNegative int level) {
        return pollFromLevel((int)currentThread().threadId(), level);
    }

    /**
     * Get a previously pooled object at the given length {@code level}, preferring objects pooled
     * in the queue that has affinity to the thread identified by {@code threadId}.
     *
     * @param threadId {@link Thread#threadId()} for {@link Thread#currentThread()}
     * @param level the length level: {@code ceil(log2(length))}
     * @return a previously pooled object or {@code null}. If non-null, the object length
     *         will be {@code 1<<level} or {@code 0} if {@code level==32}.
     */
    public final @Nullable T pollFromLevel(int threadId, @NonNegative int level) {
        if (PoolEvent.Get.ENABLED)
            PoolEvent.Get.record(this, level2len(level));
        if (level == 32 && zero != null)
            return zero;
        int lBase = level<<MD_LEVEL_SHIFT;
        return getFromStack(md, mdb(threadId)+lBase, MD_SHARED_BASE+lBase, pool);
    }

    /* --- --- --- create --- --- --- */

    /** Equivalent to {@link #createAtLeast(int, int)} */
    public final T createAtLeast(@NonNegative int len) {
        int level = len2level(len);
        if ((pooledLevels&(1<<level)) == 0)
            return factory.apply(len); // if not pooled, do not round up
        return createFromLevel(level);
    }

    /** Equivalent to {@link #createFromLevel(int, int)}, but computes {@code level}
     * from {@code len}. */
    public final T createAtLeast(int threadId, @NonNegative int len) {
        int level = len2level(len);
        if ((pooledLevels&(1<<level)) == 0)
            return factory.apply(len); // if not pooled, do not round up
        return createFromLevel(threadId, level);
    }

    /** Equivalent to {@link #createFromLevel(int, int)} */
    public final T createFromLevel(@NonNegative int level) {
        return createFromLevel((int)currentThread().threadId(), level);
    }

    /**
     * Equivalent to {@link #pollFromLevel(int, int)}, but will instantiate a new object
     * instead of returning {@code null}.
     *
     * @param threadId see {@link #pollFromLevel(int, int)}
     * @param level see {@link #pollFromLevel(int, int)}
     * @return a pooled or newly created object with length {@code 1<<level}
     *        (or 0 if {@code level == 32})
     */
    public final T createFromLevel(int threadId, @NonNegative int level) {
        int lBase = level<<MD_LEVEL_SHIFT;
        if (PoolEvent.Get.ENABLED)
            PoolEvent.Get.record(this, level2len(level));
        T o = getFromStack(md, mdb(threadId)+lBase, MD_SHARED_BASE+lBase, pool);
        if (o == null) {
            int len = level2len(level);
            if (PoolEvent.ENABLED && (len > 0 || zero == null))
                PoolEvent.EmptyPool.record(this, len);
            o = factory.apply(len);
        }
        return o;
    }

    /* --- --- --- offer --- --- --- */

    public final T zero() { return zero != null ? zero : factory.apply(0); }

    public final void setZero(@NonNull T zero) { this.zero = zero; }

    /** Equivalent to {@link #offerToLevel(int, Object, int)}. */
    public final @Nullable T offer(T o, int len) {
        return offerToLevel(o, len2level(len));
    }

    /** Equivalent to {@link #offerToLevel(int, Object, int)}. */
    public final @Nullable T offer(int threadId, T o, int len) {
        return offerToLevel(threadId, o, len2level(len));
    }
    /** Equivalent to {@link #offerToLevel(int, Object, int)}. */
    public final @Nullable T offerToLevel(T o, int level) {
        return offerToLevel((int)currentThread().threadId(), o, level);
    }

    /**
     * Offers {@code o} to the shared LIFO queue. If the shared queue is full, {@code o} will
     * be offered to thread-affinity LIFO queues.
     *
     * @param o an object to be pooled
     * @param level the result of {@link #len2level(int)} given {@code o} length
     * @return the object now owned by the caller: {@code o} if the pool is full or {@code null}
     *         if {@code o} has been taken into the pool
     */
    public final @Nullable T offerToLevelShared(T o, int level) {
        if (o == null || (pooledLevels&(1<<level)) == 0)
            return o;
        int lBase = level<<MD_LEVEL_SHIFT;
        if (offerToStack(md, MD_SHARED_BASE+lBase, pool, o) == null)
            return null;
        int surrogate = THREADS_MASK&(int)(Timestamp.nanoTime()>>16);
        return offerToOtherThreads(surrogate, lBase, o);
    }

    /**
     * Offers {@code o} a LIFO queue for the given {@code level}. {@code o} will be first offered
     * to a queue that has affinity to the given {@code threadId}, before being offered to a
     * shared queue and to queues that have affinity with other threads.
     *
     * @param threadId {@link Thread#threadId()} for {@link Thread#currentThread()}
     * @param o the object to pool its length must be 0 if {@code level == 32} or {@code 1<<level}
     * @param level the length level corresponding to {@code o} length.
     */
    public final @Nullable T offerToLevel(int threadId, T o, int level) {
        if (PoolEvent.Offer.ENABLED)
            PoolEvent.Offer.record(this, level2len(level));
        if (o == null || (pooledLevels&(1<<level)) == 0)
            return o;
        int lb = level<<MD_LEVEL_SHIFT;
        if (offerToStack(md, mdb(threadId)+lb, MD_SHARED_BASE+lb, pool, o) == null)
            return null;
        if (PoolEvent.ENABLED && (level != 32 || zero == null))
            PoolEvent.OfferToLocals.record(this, len2level(level));
        return offerToOtherThreads(threadId, lb, o);
    }

    private @Nullable T offerToOtherThreads(int threadId, int levelBase, T o) {
        if (o == zero)
            return null;
        int endBucket = threadId&THREADS_MASK;
        for (int t = endBucket; o != null && (t=((t+1)&THREADS_MASK)) != endBucket; )
            o = offerToStack(md, mdb(t)+levelBase, pool, o);
        if (o != null)
            PoolEvent.FullPool.record(this, 1<<(levelBase>>MD_LEVEL_SHIFT));
        return o;
    }

    /* --- --- --- offerToNearest --- --- --- */

    /** Equivalent to {@link #offerToNearestLevel(int, Object, int)}. */
    public final @Nullable T offerToNearest(T o, int len) {
        return offerToNearestLevel(o, len2level(len));
    }

    /** Equivalent to {@link #offerToNearestLevel(int, Object, int)}. */
    public final @Nullable T offerToNearest(int threadId, T o, int len) {
        return offerToNearestLevel(threadId, o, len2level(len));
    }

    /** Equivalent to {@link #offerToNearestLevel(int, Object, int)}. */
    public final @Nullable T offerToNearestLevel(T o, int level) {
        return offerToNearestLevel((int)currentThread().threadId(), o, level);
    }

    /**
     * Equivalent to {@link #offerToLevel(int, Object, int)}, followed by an offer to the
     * neighboring lower-length level, if {@code level} is not already {@code 0} ({@code len==1})
     * or {@code 32} ({@code length == 0})..
     *
     * @param threadId {@link Thread#threadId()} for {@link Thread#currentThread()}.
     * @param o the object to offer
     * @param level the level, which must correspond to the length of {@code o}.
     * @return the object whose ownership the caller retained: {@code null} if {@code o} was
     *         take by the pool, {@code o} if the pool had no free space.
     */
    public final @Nullable T offerToNearestLevel(int threadId, T o, int level) {
        if (offerToLevel(threadId, o, level) == null)
            return null;
        return (level&31) > 0 ? offerToLevel(threadId, o, level-1) : o;
    }

    /* --- --- --- prime --- --- --- */


    public void primeLevel(@NonNegative int level) {
        primeLevel(factory, level, 2, 0);
    }

    public void primeLevel(@NonNegative int level, double localMultiplier, int additional) {
        primeLevel(factory, level, localMultiplier, additional);
    }

    public void primeLevel(IntFunction<T> factory, @NonNegative int level) {
        primeLevel(factory, level, 2, 0);
    }

    public void primeLevel(IntFunction<T> factory, @NonNegative int level, double localMultiplier,
                           int additional) {
        //noinspection ConstantValue
        if (level < 0 || level > 32)
            throw new IllegalArgumentException("level not in [0, 32]");
        int lBase      = level<<MD_LEVEL_SHIFT;
        int shMdb      = MD_SHARED_BASE+lBase;
        int upLocalCap = Math.max(1, md[mdb(0)+lBase+MD_CAP]);
        int len        = level == 32 ? 0 : 1<<level;
        Supplier<T> lFac = () -> factory.apply(len);
        int goal   = Math.min(md[shMdb+MD_CAP],
                              additional + (int)(localMultiplier*(THREADS*upLocalCap)));
        int needed = goal - md[shMdb+MD_SIZE];
        int chunk  = needed/THREADS;
        if (needed <= 0)
            return; // no work

        Semaphore start = new Semaphore(0), done = new Semaphore(0);
        for (int i = 0; i < THREADS; i++) {
            int thread = i;
            ForkJoinPool.commonPool().execute(() -> {
                int add = thread > 0 ? chunk : needed-(chunk*(THREADS-1));
                start.acquireUninterruptibly();
                if (add > 0)
                    concurrentPrimeStack(md, shMdb, pool, lFac, add);
                done.release();
            });
        }
        // increases the likelihood that factory.get() gets called from THREADS distinct TLABs,
        // which will prevent false sharing between threads that map do different local LIFOs
        start.release(THREADS);
        done.acquireUninterruptibly(THREADS);
    }


    /* --- --- --- LeakyPool --- --- --- */

    @Override public void cleanLeakyRefs() {
        int endMdb=MD_SHARED_BASE+MD_THREAD_WIDTH;
        for (int mdb = MD_BASE; mdb < endMdb; mdb+=MD_LEVEL_WIDTH)
            cleanStackLeakyRefs(md, mdb, pool);
    }
}
