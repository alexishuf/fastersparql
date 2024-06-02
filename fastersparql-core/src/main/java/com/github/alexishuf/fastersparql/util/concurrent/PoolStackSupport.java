package com.github.alexishuf.fastersparql.util.concurrent;

import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.max;
import static java.lang.System.arraycopy;
import static java.lang.Thread.onSpinWait;

class PoolStackSupport {
    private static final boolean DEBUG = PoolStackSupport.class.desiredAssertionStatus();
    private static final int RUNTIME_THREADS      = Runtime.getRuntime().availableProcessors();
    private static final int SAFE_RUNTIME_THREADS = Math.max(2, RUNTIME_THREADS);
    private static final int CHUNK          = 8;  // half a cache line
    private static final int CHUNK_REQUIRED = CHUNK+(SAFE_RUNTIME_THREADS-1);
    private static final VarHandle MD       = MethodHandles.arrayElementVarHandle(int[].class);

    public static final int THREADS       = 1<<(32-numberOfLeadingZeros(SAFE_RUNTIME_THREADS-1));
    public static final int THREADS_MASK  = THREADS-1;
    public static final int THREAD_WIDTH  = 32; // 2 64-byte cache lines and 4-byte references
    public static final int MD_SIZE  = 1;
    public static final int MD_BEGIN = 2;
    public static final int MD_CAP   = 3;

    public static int localCapacity(int sharedCapacity) {
        int localCapacity = Math.min(CHUNK, sharedCapacity/THREADS);
        if (localCapacity*THREADS >= sharedCapacity)
            localCapacity /= 2;
        return localCapacity;
    }


    public static <T> void checkInitialized(int[] md, int firstMdb, int mdbStep, T[] pool) {
        int objects = 0;
        for (int i = firstMdb; i < md.length; i += mdbStep) {
            if (md[i+MD_SIZE] != 0)
                throw new IllegalStateException("size != 0");
            if (md[i+MD_BEGIN] < objects)
                throw new IllegalStateException("begin overlaps previous stack");
            int cap = md[i + MD_CAP];
            if (cap < 0)
                throw new IllegalStateException("negative capacity");
            objects += cap;
        }
        if (pool.length < objects)
            throw new IllegalStateException("pool.length < cumulative capacity");
    }

    public static <T> @Nullable T getFromStack(int[] md, int mdb, int sharedMdb, T[] pool) {
        int begin = md[mdb+MD_BEGIN];
        T o = null;
        while (!MD.weakCompareAndSetAcquire(md, mdb, 0, 1)) onSpinWait();
        // this thread owns the stack lock
        int tail = md[mdb+MD_SIZE]-1;
        if (tail >= 0) { // local stack is not empty
            o = pool[begin+tail];
            md[mdb+MD_SIZE] = tail;
        } else if (md[sharedMdb+MD_SIZE] > 0) { // shared stack appears not empty
            int shBegin = md[sharedMdb+MD_BEGIN];
            while (!MD.weakCompareAndSetAcquire(md, sharedMdb, 0, 1)) onSpinWait();
            // this thread owns the lock for the shared stack
            tail = md[sharedMdb+MD_SIZE]-1;
            if (tail >= 0) { // shared stack is really not empty
                o = pool[shBegin+tail]; // take last item to satisfy this request
                if (tail >= CHUNK_REQUIRED && md[mdb+MD_CAP] >= CHUNK) {
                    // take a chunk now to avoid future contention on the shared stack
                    arraycopy(pool, shBegin+tail-CHUNK, pool, begin, CHUNK);
                    md[sharedMdb+MD_SIZE] = tail-CHUNK;
                    md[mdb      +MD_SIZE] = CHUNK;
                } else {
                    md[sharedMdb+MD_SIZE] = tail; // decrement size for item taken
                }
            }
            MD.setRelease(md, sharedMdb, 0); // unlock shared stack
        }
        MD.setRelease(md, mdb, 0); // unlock local stack
        if (DEBUG)
            requireRecycled(o);
        return o;
    }


    public static <T> @Nullable T offerToStack(int[] md, int mdb, T[] pool, T o) {
        if (DEBUG)
            requireRecycled(o);
        int begin = md[mdb+MD_BEGIN], cap = md[mdb+MD_CAP];
        if (!MD.weakCompareAndSetAcquire(md, mdb, 0, 1)) { // did not lock, other thread owns it
            if (md[mdb+MD_SIZE] >= cap)
                return o; // do not spin: stack is full
            while (!MD.weakCompareAndSetAcquire(md, mdb, 0, 1)) onSpinWait();
        }
        // this thread owns the stack lock
        int size = md[mdb+MD_SIZE];
        if (size < cap) {
            pool[begin+size   ] = o;
            md  [mdb  +MD_SIZE] = size+1;
            o = null;
        }
        MD.setRelease(md, mdb, 0); // unlock
        return o;
    }

    private static <T> void requireRecycled(T o) {
        if (o instanceof Owned<?> owned)
            owned.requireOwner(RECYCLED);
    }

    public static <T> int count(int[] md, int mdb, int mdbStep, T[] pool, T o) {
        int count = 0;
        for (; mdb < md.length; mdb += mdbStep) {
            int begin = md[mdb + MD_BEGIN];
            int end = begin+(int)MD.getAcquire(md, mdb+MD_SIZE);
            for (int i = begin; i < end; ++i) {
                if (pool[i] == o) {
                    while (!MD.weakCompareAndSetAcquire(md, mdb, 0, 1)) onSpinWait();
                    end = begin+(int)MD.getAcquire(md, mdb+MD_SIZE);
                    if (i < end && pool[i] == o)
                        ++count;
                    MD.setRelease(md, mdb, 0);
                }
            }
        }
        return count;
    }

    static {
        assert localCapacity(THREADS*32) <= CHUNK
                : "offerToStack() implementation assumes maximum local capacity is CHUNK";
    }

    public static <T> @Nullable T offerToStack(int[] md, int mdb, int sharedMdb, T[] pool, T o) {
        if (DEBUG)
            requireRecycled(o);
        int cap = md[mdb+MD_CAP], begin = md[mdb+MD_BEGIN];
        // offer to local
        if (md[mdb+MD_SIZE] < cap && doOfferToStack(md, begin, cap, mdb, pool, o))
            return null; // taken
        // local is full. Offer o to shared
        if (doOfferToStack(md, md[sharedMdb+MD_BEGIN], md[sharedMdb+MD_CAP], sharedMdb, pool, o))
            return null; // taken
        return o; // both stacks are full
    }

    private static <T> boolean doOfferToStack(int[] md, int begin, int cap,
                                              int mdb, T[] pool, T o) {
        while (!MD.weakCompareAndSetAcquire(md, mdb, 0, 1)) onSpinWait();
        int     size  = md[mdb+MD_SIZE];
        boolean taken = size < cap;
        if (taken) {
            pool[begin+size] = o;
            md[mdb+MD_SIZE]  = size+1;
        }
        MD.setRelease(md, mdb, 0);
        return taken;
    }

    public static <T> void concurrentPrimeStack(int[] md, int mdb, T[] pool,
                                                Supplier<T> factory, int addItems) {
        int begin = md[mdb+MD_BEGIN], cap = md[mdb+MD_CAP];
        while (addItems-- > 0) {
            while (!MD.weakCompareAndSetAcquire(md, mdb, 0, 1)) onSpinWait();
            try {
                int size = md[mdb + MD_SIZE];
                if (size >= cap)
                    break;
                pool[begin+size] = factory.get();
                md[mdb+MD_SIZE] = size+1;
            } finally {
                MD.setRelease(md, mdb, 0);
            }
        }
    }

    public static <T> void cleanStackLeakyRefs(int[] md, int mdb, T[] pool) {
        int i = 0, begin = md[mdb+MD_BEGIN], end = begin+md[mdb+MD_CAP];
        // clear refs in chunks of 16
        for (boolean hasWork = true; hasWork; ) {
            hasWork = false;
            while (!MD.weakCompareAndSetAcquire(md, mdb, 0, 1)) onSpinWait();
            // stack is locked by this thread
            i = max(begin+md[mdb+MD_SIZE], i); // continue from i or jump to new top
            for (int e = Math.min(i+16, end); i < e && (hasWork = pool[i] != null); ++i)
                pool[i] = null;
            MD.setRelease(md, mdb, 0); // unlock stack
        }
    }
}
