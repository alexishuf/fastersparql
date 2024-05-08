package com.github.alexishuf.fastersparql.util.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Array;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.concurrent.PoolStackSupport.*;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.lang.Thread.currentThread;

public final class Alloc<T> implements LeakyPool, StatsPool, JournalNamed {
    public static final int THREADS = PoolStackSupport.THREADS;
    private static final int POOL_PADD = 128/4;
    private static final int MD_WIDTH = 128/4;
    private static final int MD_SHIFT = numberOfTrailingZeros(MD_WIDTH);
    private static final int MD_SHARED = (1+THREADS)*MD_WIDTH;

    private final int[] md;
    private final T[] pool;
    private final Supplier<T> factory;
    private final Class<T> cls;
    private final String journalName;
    private final int bytesPerObject;

    @SuppressWarnings("unchecked")
    public Alloc(Class<T> cls, @Nullable String name, int sharedCapacity, Supplier<T> factory,
                 int bytesPerObject) {
        this.cls = Objects.requireNonNull(cls);
        this.journalName = name != null ? name
                         : format("Alloc(%s)@%x", cls.getSimpleName(), identityHashCode(this));
        this.bytesPerObject = bytesPerObject;
        this.factory = Objects.requireNonNull(factory);
        int poolSize = POOL_PADD + THREADS*THREAD_WIDTH + sharedCapacity;
        this.pool = (T[])Array.newInstance(cls, poolSize);
        this.md = new int[MD_WIDTH*(THREADS+2)];
        int begin = POOL_PADD;
        int localCapacity = localCapacity(sharedCapacity);
        assert localCapacity <= THREAD_WIDTH;
        for (int i = 0; i < THREADS; i++) {
            int mdb = mdb(i);
            assert mdb >= MD_WIDTH &&  mdb < MD_SHARED;
            md[mdb+MD_BEGIN] = begin;
            md[mdb+MD_CAP  ] = localCapacity;
            begin += THREAD_WIDTH;
        }
        assert begin == POOL_PADD+THREADS*THREAD_WIDTH;
        md[MD_SHARED+MD_BEGIN] = begin;
        md[MD_SHARED+MD_CAP  ] = sharedCapacity;
        checkInitialized(md, MD_WIDTH, MD_WIDTH, pool);
        PoolCleaner.monitor(this);
        PoolStats.monitor(this);
    }

    private static int mdb(int threadId) {
        return (1+(threadId&THREADS_MASK)) << MD_SHIFT;
    }

    @Override public String toString() {return journalName();}

    @Override public String journalName() {return journalName;}

    /** Maximum number of objects that can be held in the LIFO queue that is polled when a
     *  thread meets its preferred queue empty.  */
    public int sharedCapacity() { return md[MD_SHARED+MD_CAP]; }

    /** Maximum number of objects a thread-affinity queue can hold */
    public int perThreadCapacity() { return md[mdb(0)+MD_CAP]; }

    public Class<T> itemClass() { return cls; }

    //@SuppressWarnings("BooleanMethodIsAlwaysInverted")
    //public boolean debugContains(T o) {
    //    return count(md, 0, MD_WIDTH, pool, o) > 0;
    //}

    /** Equivalent to {@link #poll(int)}. */
    public @Nullable T poll() {
        return getFromStack(md, mdb((int)currentThread().threadId()), MD_SHARED, pool);
    }

    /**
     * Get a pooled object, preferrably from a queue that has affinity with the thread
     * identified by {@code threadId}.
     *
     * @param threadId  {@link Thread#threadId()} for {@link Thread#currentThread()}
     * @return {@code null} or a previously pooled object
     */
    public @Nullable T poll(int threadId) {
        return getFromStack(md, mdb(threadId), MD_SHARED, pool);
    }

    /** Equivalent to {@link #create(int)}. */
    public T create() { return create((int)currentThread().threadId()); }

    /**
     * Similar to {@link #poll(int)}, but will create a new object instead of retuning {@code null}.
     * @param threadId {@link Thread#threadId()} for {@link Thread#currentThread()}
     * @return a pooled or new object.
     */
    public T create(int threadId) {
        T o = getFromStack(md, mdb(threadId), MD_SHARED, pool);
        if (o == null) {
            PoolEvent.EmptyPool.record(this, 1);
            o = factory.get();
        }
        return o;
    }

    /**
     * Offers {@code o} to be returned in future {@code poll/create()} calls.
     *
     * @param o an object to be pooled
     * @return the object which the caller can retain ownership: {@code o}if the pool was full
     *         or {@code null} if {@code o} was taken by the pool.
     */
    public @Nullable T offer(T o) {
        return offer((int)currentThread().threadId(), o);
    }

    /**
     * Offers {@code o} to be returned in future {@code poll/create()} calls. {@code o} will be
     * first offered to the LIFO queue that has affinity with the thread identified by
     * {@code threadId}
     *
     * @param threadId {@link Thread#threadId()} for {@link Thread#currentThread()}
     * @param o an object to be pooled
     * @return the object which the caller can retain ownership: {@code o}if the pool was full
     *         or {@code null} if {@code o} was taken by the pool.
     */
    public @Nullable T offer(int threadId, T o) {
        if (offerToStack(md, mdb(threadId), MD_SHARED, pool, o) == null)
            return null;
        PoolEvent.OfferToLocals.record(this, 1);
        return offerToOthers(threadId, o);
    }

    /**
     * Offers {@code o} first to the shared LIFO queue, so it can be returned by future
     * {@code poll/create()} calls. this differs from other {@code offer()} methods, which will
     * first offer to the LIFO queue that has affinity with the current (or given) thread.
     *
     * @param o the object to be pooled
     * @return the object which the caller retains ownership: {@code o} itself if the pool was
     *         full or {@code null} if {@code o} was taken into the pool.
     */
    public @Nullable T offerToShared(T o) {
        if (offerToStack(md, MD_SHARED, pool, o) == null)
            return null;
        int surrogate = (int)(Timestamp.nanoTime() >> 16);
        return offerToOthers(surrogate, o);
    }

    @Override public String name()          {return journalName;}
    @Override public int    sharedObjects() {return md[MD_SHARED+MD_SIZE];}
    @Override public int    sharedBytes()   {return sharedObjects()*bytesPerObject;}
    @Override public int     localBytes()   {return  localObjects()*bytesPerObject;}

    @Override public int localObjects() {
        int n = 0;
        for (int thread = 0; thread < THREADS; thread++)
            n += md[mdb(thread)+MD_SIZE];
        return n;
    }

    public void prime() { prime(factory, 2, 0); }

    public void prime(double localMultiplier, int additional) {
        prime(factory, localMultiplier, additional);
    }

    public void prime(Supplier<T> factory, double localMultiplier, int additional) {
        int upLocal = Math.max(1, md[mdb(0)+MD_CAP]);
        int goal    = Math.min(md[MD_SHARED+MD_CAP],
                               additional + (int)(localMultiplier*(THREADS*upLocal)));
        int needed  = goal - md[MD_SHARED+MD_SIZE];
        int chunk   = needed/THREADS;
        if (needed <= 0)
            return; // no work
        Semaphore start = new Semaphore(0), done = new Semaphore(0);
        for (int i = 0; i < THREADS; ++i) {
            int thread = i;
            ForkJoinPool.commonPool().submit(() -> {
                start.acquireUninterruptibly();
                int add = thread > 0 ? chunk : needed-((THREADS-1)*chunk);
                if (add > 0)
                    concurrentPrimeStack(md, MD_SHARED, pool, factory, add);
                done.release();
            });
        }
        // This will increase the likelihood that primeStack() runs from all TLABs.
        // Using a distinct TLABs for each local FIFO prevents false sharing
        start.release(THREADS);
        done.acquireUninterruptibly(THREADS);
    }

    private @Nullable T offerToOthers(int threadId, T o) {
        int endBucket = threadId&THREADS_MASK;
        for (int i=endBucket; o != null && (i=(i+1)&THREADS_MASK) != endBucket; )
            o = offerToStack(md, mdb(i), pool, o);
        if (o != null)
            o = offerToStack(md, MD_SHARED, pool, o);
        if (o != null)
            PoolEvent.FullPool.record(this, 1);
        return o;
    }

    @Override public void cleanLeakyRefs() {
        for (int mdb = MD_WIDTH; mdb <= MD_SHARED; mdb += MD_WIDTH)
            cleanStackLeakyRefs(md, mdb, pool);
    }

    public StringBuilder dump(StringBuilder sb) {
        sb.append(name());
        sb.append("{sharedCap=").append(md[MD_SHARED+MD_CAP]);
        sb.append(" sharedSize=").append(md[MD_SHARED+MD_SIZE]);
        sb.append(" localCap=").append(md[MD_WIDTH+MD_CAP]);
        sb.append(" localSizes=[");
        for (int thread = 0; thread < THREADS; thread++)
            sb.append(md[mdb(thread)+MD_SIZE]).append(' ');
        sb.setLength(sb.length()-1);
        return sb.append("]}");
    }

    @SuppressWarnings("unused") // used only for testing & debug
    public String dump() { return dump(new StringBuilder()).toString(); }
}
