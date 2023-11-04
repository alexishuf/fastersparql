package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.util.concurrent.LeakyPool;
import com.github.alexishuf.fastersparql.util.concurrent.PoolCleaner;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Array;

import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Thread.onSpinWait;

public class BatchPool<B extends Batch<B>> implements LeakyPool {
    private static final VarHandle I = MethodHandles.arrayElementVarHandle(int[].class);

    private static final int CTRL_WIDTH      = 64/4;
    private static final int CTRL_SHIFT      = numberOfTrailingZeros(CTRL_WIDTH);
    private static final int CTRL_BASE       = 128/4;
    private static final int CTRL_SIZE_OFF   = 1;
    private static final int CTRL_CAP_OFF    = 2;
    private static final int NEIGHBOR_VISITS = 6;
    private static final int TOTAL_VISITS    = 2+NEIGHBOR_VISITS;
    private static final int SHARED_DELTA    = Integer.MIN_VALUE;
    private static final int MIN_THREADS     = 1 << (32 - numberOfLeadingZeros(NEIGHBOR_VISITS));
            static final int LOCAL_CAP       = 128/4;
    private static final int LOCAL_CAP_SHIFT = numberOfTrailingZeros(LOCAL_CAP);
    private static final int BATCHES_BASE    = 128/4;

    static {
        assert Integer.bitCount(CTRL_WIDTH) == 1 : "CTRL_WIDTH now a power of 2";
        assert Integer.bitCount(LOCAL_CAP ) == 1 : "LOCAL_CAP  now a power of 2";
        //noinspection ConstantValue
        assert CTRL_BASE > TOTAL_VISITS : "CTRL_BASE too low";
    }

    public interface Factory<B extends Batch<B>> { B create(); }

    private final int threadMask;
    private final int[] control;
    private final B[] batches;
    private final Factory<B> factory;
    private final Class<B> batchClass;

    @SuppressWarnings("unchecked")
    public BatchPool(Class<B> cls, Factory<B> factory, int threads, int sharedCapacity) {
        threads           = 1 << (32 - numberOfLeadingZeros(Math.max(threads, MIN_THREADS)-1));
        int batchesLen    = BATCHES_BASE + sharedCapacity + threads*LOCAL_CAP;
        this.batchClass   = cls;
        this.threadMask   = (threads-1);
        this.factory      = factory;
        this.batches      = (B[])Array.newInstance(cls, batchesLen);
        this.control      = new int[CTRL_BASE+(threads+1)*CTRL_WIDTH];
        for (int stack = 0, nStacks = threads; stack < nStacks; stack++)
            control[ctrlIdx(stack)+CTRL_CAP_OFF] = LOCAL_CAP;
        control[ctrlIdx(threads)+CTRL_CAP_OFF] = sharedCapacity; // last stack is the shared stack
        control[0] =  0;           // first visit stack with thread affinity
        control[1] = SHARED_DELTA; // second visit is shared stack
        control[2] =  1;           // visit neighbors in a circular motion
        control[3] =  2;
        control[4] =  3;
        control[5] = -3;
        control[6] = -2;
        control[7] = -1;
        //noinspection ConstantValue
        assert TOTAL_VISITS == 8;
        assert MIN_THREADS >  NEIGHBOR_VISITS;
        prime();
        PoolCleaner.INSTANCE.monitor(this);
    }

    private static int ctrlIdx(int stack) { return    CTRL_BASE+(stack<<CTRL_SHIFT); }
    private static int   begin(int stack) { return BATCHES_BASE+(stack<<LOCAL_CAP_SHIFT); }

    private void prime() {
        for (int stack = 0; stack <= threadMask; stack++) {
            for (int i = 0; i < LOCAL_CAP; i++)
                offerToStack(stack, factory.create());
        }
        int sharedStack = threadMask+1;
        int sharedCap = control[ctrlIdx(sharedStack) + CTRL_CAP_OFF];
        int sharedInit = Math.min(sharedCap, (threadMask+1)*4*LOCAL_CAP);
        for (int i = 0; i < sharedInit; i++)
            offerToStack(sharedStack, factory.create());
    }

    @Override public void cleanLeakyRefs() {
        B[] batches = this.batches;
        for (int stack = 0, nStacks = threadMask+1; stack < nStacks; stack++) {
            int ci = ctrlIdx(stack), base = begin(stack);
            // concurrently scan end of post-size non-null references
            int begin = base+control[ci+CTRL_SIZE_OFF], mid = begin;
            int end   = base+control[ci+CTRL_CAP_OFF];
            while (mid < end && batches[mid] != null)
                ++mid;
            // clear leaky references while locked
            while ((int)I.compareAndExchangeAcquire(control, ci, 0, 1) != 0) onSpinWait();
            try {
                for (int i = base+control[ci+CTRL_SIZE_OFF]; i < mid; ++i)
                    batches[i] = null;
            } finally {
                I.setRelease(control, ci, 0);
            }
        }
    }

    public Class<B> batchClass() { return batchClass; }

    /**
     * Equivalent to {@link #get(int)} using {@link Thread#threadId()} of the
     * {@link Thread#currentThread()}
     *
     * @return see {@link #get(int)}
     */
    public @NonNull B get() { return get((int)Thread.currentThread().threadId()); }

    /**
     * Get a pooled batch or instantiate a new one.
     *
     * @param threadId arbitrary number that should be constant for a {@link Thread} and
     *                 ideally unique to the current {@link Thread} making this call
     *                 (i.e., {@link Thread#threadId()}).
     * @return A pooled or new {@code B} instance.
     */
    public @NonNull B get(int threadId) {
        B b = getFromStack(threadId & threadMask);
        if (b == null) {
            if ((b = getFromStack(threadMask+1)) == null)
                b = factory.create();
        }
        return b;
    }

    /**
     * Equivalent to {@link #offer(int, Batch)} using {@link Thread#threadId()} of the
     * {@link Thread#currentThread()}.
     *
     * @param b see {@link #offer(int, Batch)}
     * @return see {@link #offer(int, Batch)}
     */
    public @Nullable B offer(B b) { return offer((int)Thread.currentThread().threadId(), b); }

    /**
     * Try atomically adding {@code b} to this pool. Call as {@code b = pool.offer(threadId, b);}
     *
     * @param threadId A constant value for the current {@link Thread}, and ideally unique
     *                 (i.e., {@link Thread#threadId()}). This will be used to select the
     *                 preferred pool with most affinity to the current thread, which will
     *                 receive the first offer of {@code b}.
     * @param b the batch to be added, if {@code null} this call will have no effect.
     * @return {@code null} if {@code b} was taken into the pool, {@code b} if the pool is
     *         full and the caller remains the owner of {@code b}
     */
    public @Nullable B offer(int threadId, B b) {
        for (int i = 0; i < TOTAL_VISITS && b != null; i++)
            b = offerToStack(i == 1 ? threadMask+1 : (threadId+control[i])&threadMask, b);
        return b;
    }

    /** Exposed only for testing <strong>DO NOT CALL</strong> */
    @Nullable B getFromStack(int stack) {
        int cl = ctrlIdx(stack), cs = cl+CTRL_SIZE_OFF, begin = begin(stack), newSize;
        B b;
        if ((int)I.compareAndExchangeAcquire(control, cl, 0, 1) != 0) {
            if (control[cs] == 0) return null;
            while ((int)I.compareAndExchangeAcquire(control, cl, 0, 1) != 0) onSpinWait();
        }
        if ((newSize = control[cs]-1) < 0) {
            b = null;
        } else {
            b = batches[begin+newSize];
            control[cs] = newSize;
        }
        I.setRelease(control, cl, 0);
        return b;
    }

    /** Exposed only for testing <strong>DO NOT CALL</strong> */
    @Nullable B offerToStack(int stack, B b) {
        int cl = ctrlIdx(stack), cs = cl+CTRL_SIZE_OFF, begin = begin(stack), size;
        if ((int)I.compareAndExchangeAcquire(control, cl, 0, 1) != 0) {
            if (stack < threadMask && control[cs] >= LOCAL_CAP) return b;
            while ((int)I.compareAndExchangeAcquire(control, cl, 0, 1) != 0) onSpinWait();
        }
        if ((size = control[cs]) < control[cl+CTRL_CAP_OFF]) {
            batches[begin+size] = b;
            control[cs] = size+1;
            b = null;
        }
        I.setRelease(control, cl, 0);
        return b;
    }
}
